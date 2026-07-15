const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const mqtt = require('mqtt');
const { pool, initDb } = require('./db');

// Keep track of registered device IDs to filter out public broker spam
const registeredDevices = new Set(['mytank123']);

async function loadRegisteredDevices() {
    try {
        const res = await pool.query('SELECT device_id FROM w_device_config');
        res.rows.forEach(row => registeredDevices.add(row.device_id));
        console.log(`[MQTT Server Listener] Loaded ${registeredDevices.size} registered devices from database:`, Array.from(registeredDevices));
    } catch (err) {
        console.error('[MQTT Server Listener] Error loading registered devices:', err);
    }
}

// Initialize database and load registered devices
(async () => {
    await initDb();
    await loadRegisteredDevices();
})();

// MQTT Listener in the background
const mqttBroker = 'mqtt://broker.hivemq.com:1883';
const mqttClient = mqtt.connect(mqttBroker, {
    clientId: 'hydrosync-server-' + Math.random().toString(16).substr(2, 6)
});

// Memory cache to aggregate simultaneous telemetry feeds
const deviceCache = {};

mqttClient.on('connect', () => {
    console.log('[MQTT Server Listener] Connected to HiveMQ Broker.');
    // Subscribe to wildcard status topics: device_prefix/status/metric_name
    mqttClient.subscribe('+/status/+', (err) => {
        if (err) {
            console.error('[MQTT Server Listener] Wildcard subscription failed:', err);
        } else {
            console.log('[MQTT Server Listener] Subscribed to +/status/+ successfully.');
        }
    });
});

mqttClient.on('message', async (topic, message) => {
    try {
        const topicParts = topic.split('/');
        if (topicParts.length !== 3 || topicParts[1] !== 'status') return;

        const deviceId = topicParts[0];

        // Filter out unregistered devices to protect database from public HiveMQ spam
        if (!registeredDevices.has(deviceId)) {
            return;
        }

        const metric = topicParts[2]; // level, volume, data_usage, version, or ip
        const valStr = message.toString().trim();

        if (!deviceCache[deviceId]) {
            deviceCache[deviceId] = {
                level: null,
                volume: null,
                data_usage: 0,
                version: '1.6',
                last_insert_time: 0
            };
        }

        // Handle version status messages as strings (can contain '+', letters, or numbers)
        if (metric === 'version') {
            deviceCache[deviceId].version = valStr;
            console.log(`[MQTT Server Listener] Device ${deviceId} reported Firmware Version: ${valStr}`);
            return;
        }

        const value = parseFloat(valStr);
        if (isNaN(value)) return;

        // Update cache
        if (metric === 'level') {
            deviceCache[deviceId].level = value;
        } else if (metric === 'volume') {
            deviceCache[deviceId].volume = value;
        } else if (metric === 'data_usage') {
            deviceCache[deviceId].data_usage = parseInt(valStr, 10) || 0;
        }

        const cache = deviceCache[deviceId];
        const now = Date.now();

        // Check if both level and volume are populated, and 5 seconds has passed since last DB entry to throttle
        if (cache.level !== null && cache.volume !== null && (now - cache.last_insert_time >= 5000)) {
            cache.last_insert_time = now;
            
            const levelVal = cache.level;
            const volumeVal = cache.volume;
            const dataUsageVal = cache.data_usage || 0;
            const versionVal = cache.version || '1.6';

            // Clear cache immediately BEFORE the async db call to prevent concurrent/race condition inserts
            cache.level = null;
            cache.volume = null;

            await pool.query(
                `INSERT INTO w_telemetry (device_id, level, volume, data_usage, version) VALUES ($1, $2, $3, $4, $5)`,
                [deviceId, levelVal, volumeVal, dataUsageVal, versionVal]
            );
            console.log(`[DB Log] Successfully saved telemetry for ${deviceId}: Level=${levelVal}cm, Volume=${volumeVal}L, DataUsage=${dataUsageVal} Bytes, Version=${versionVal}`);
            
            // Check for threshold alerts and dispatch SMS if needed
            checkDeviceThresholdAlerts(deviceId, levelVal, volumeVal).catch(err => {
                console.error('[SMS Trigger Error] Error checking threshold alerts:', err);
            });
        }
    } catch (err) {
        console.error('[MQTT Server Listener] Error processing packet:', err);
    }
});

// Helper to send JSON responses
function sendJSON(res, data, statusCode = 200) {
    res.writeHead(statusCode, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    });
    res.end(JSON.stringify(data));
}

// Robust MQTT publish helper that supports persistent background client as well as fallback to short-lived connection
let transientQueue = [];
let isProcessingTransientQueue = false;

async function processTransientQueue() {
    if (isProcessingTransientQueue || transientQueue.length === 0) return;
    isProcessingTransientQueue = true;

    console.log(`[MQTT Server] Initializing single transient connection to process ${transientQueue.length} queued messages...`);
    const transientClient = mqtt.connect(mqttBroker, {
        clientId: 'hydrosync-transient-' + Math.random().toString(16).substr(2, 6),
        connectTimeout: 8000
    });

    let connected = false;
    let completed = false;

    const finishAll = (err) => {
        if (!completed) {
            completed = true;
            try { transientClient.end(); } catch (e) {}
            
            // Resolve all currently queued items
            const itemsToResolve = [...transientQueue];
            transientQueue = [];
            isProcessingTransientQueue = false;
            
            itemsToResolve.forEach(item => {
                if (err) {
                    console.error(`[MQTT Transient Error] Failed to publish ${item.topic}:`, err);
                }
                item.resolve();
            });

            // If new items were added while finishing, run again
            if (transientQueue.length > 0) {
                processTransientQueue();
            }
        }
    };

    transientClient.on('connect', async () => {
        connected = true;
        console.log(`[MQTT Transient] Connected. Publishing ${transientQueue.length} messages...`);
        
        while (transientQueue.length > 0) {
            const item = transientQueue.shift();
            try {
                await new Promise((res, rej) => {
                    transientClient.publish(item.topic, item.payload.toString(), { qos: 1, retain: item.retain }, (err) => {
                        if (err) {
                            console.error(`[MQTT Transient Publish Error] Failed for ${item.topic}:`, err);
                            rej(err);
                        } else {
                            console.log(`[MQTT Transient Publish Success] ${item.topic}: ${item.payload}`);
                            res();
                        }
                    });
                });
                item.resolve();
            } catch (err) {
                item.resolve(); // resolve anyway to not block
            }
        }
        
        finishAll();
    });

    transientClient.on('error', (err) => {
        console.error('[MQTT Transient Connection Error]:', err);
        finishAll(err);
    });

    setTimeout(() => {
        if (!connected) {
            console.error('[MQTT Transient Connection Timeout]');
            finishAll(new Error('Connection timeout'));
        }
    }, 8000);
}

async function publishMqttMessage(topic, payload, retain = false) {
    return new Promise((resolve) => {
        if (mqttClient && mqttClient.connected) {
            mqttClient.publish(topic, payload.toString(), { qos: 1, retain: retain }, (err) => {
                if (err) {
                    console.error(`[MQTT Publish Error] Failed to publish to ${topic}:`, err);
                } else {
                    console.log(`[MQTT Publish Success] Published to ${topic}: ${payload}`);
                }
                resolve();
            });
        } else {
            console.log(`[MQTT Server] Background client not connected. Queuing message for ${topic} (payload: ${payload})...`);
            transientQueue.push({ topic, payload, retain, resolve });
            processTransientQueue();
        }
    });
}

// HTTP Server
const server = http.createServer(async (req, res) => {
    // Enable CORS for preflight options
    if (req.method === 'OPTIONS') {
        res.writeHead(204, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        });
        res.end();
        return;
    }

    const reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
    const urlPath = reqUrl.pathname;

    // API: Receive telemetry via HTTP POST (keeps server active and logs 24/7)
    if (urlPath === '/api/telemetry' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => {
            body += chunk.toString();
        });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { device_id, level, volume, data_usage, version } = data;

                if (!device_id || level === undefined || volume === undefined) {
                    sendJSON(res, { success: false, error: 'Missing required fields' }, 400);
                    return;
                }

                // Register device in memory set
                registeredDevices.add(device_id);

                const levelVal = parseFloat(level);
                const volumeVal = parseFloat(volume);
                const dataUsageVal = parseInt(data_usage, 10) || 0;
                const versionVal = version ? version.toString().trim() : '1.6';

                if (isNaN(levelVal) || isNaN(volumeVal)) {
                    sendJSON(res, { success: false, error: 'Invalid level or volume' }, 400);
                    return;
                }

                // Update or create device cache so the MQTT listener is synchronized
                if (!deviceCache[device_id]) {
                    deviceCache[device_id] = {
                        level: null,
                        volume: null,
                        data_usage: 0,
                        version: '1.6',
                        last_insert_time: 0
                    };
                }
                deviceCache[device_id].last_insert_time = Date.now();
                deviceCache[device_id].data_usage = dataUsageVal;

                // Log the telemetry point in w_telemetry
                await pool.query(
                    `INSERT INTO w_telemetry (device_id, level, volume, data_usage, version) VALUES ($1, $2, $3, $4, $5)`,
                    [device_id, levelVal, volumeVal, dataUsageVal, versionVal]
                );
                
                // Check for threshold alerts and dispatch SMS if needed
                checkDeviceThresholdAlerts(device_id, levelVal, volumeVal).catch(err => {
                    console.error('[SMS Trigger Error] Error checking threshold alerts on HTTP telemetry:', err);
                });
                
                // Get or create device configuration
                let configRes = await pool.query(
                    `SELECT tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url, api_url
                     FROM w_device_config WHERE device_id = $1`,
                    [device_id]
                );
                
                let deviceConfig = null;
                if (configRes.rows.length === 0) {
                    // Create default entry
                    await pool.query(
                        `INSERT INTO w_device_config (device_id) VALUES ($1) ON CONFLICT (device_id) DO NOTHING`,
                        [device_id]
                    );
                    deviceConfig = {
                        tank_height: 200.0,
                        sensor_height: 45.0,
                        tank_diameter: 228.0,
                        num_tanks: 1,
                        telemetry_interval: 15,
                        gsm_numbers: '',
                        ota_url: '',
                        api_url: ''
                    };
                } else {
                    deviceConfig = configRes.rows[0];
                }

                console.log(`[HTTP API Log] Successfully saved telemetry for ${device_id}: Level=${levelVal}cm, Volume=${volumeVal}L, DataUsage=${dataUsageVal} Bytes, Version=${versionVal}`);
                
                // Send JSON back to ESP32 with latest configurations & pending OTA URLs
                sendJSON(res, { 
                    success: true, 
                    message: 'Telemetry logged successfully',
                    config: {
                        tank_height: parseFloat(deviceConfig.tank_height),
                        sensor_height: parseFloat(deviceConfig.sensor_height),
                        tank_diameter: parseFloat(deviceConfig.tank_diameter),
                        num_tanks: parseInt(deviceConfig.num_tanks, 10),
                        telemetry_interval: parseInt(deviceConfig.telemetry_interval, 10),
                        gsm_numbers: deviceConfig.gsm_numbers || '',
                        ota_url: deviceConfig.ota_url || '',
                        api_url: deviceConfig.api_url || ''
                    }
                });
            } catch (err) {
                console.error('[API Error] /api/telemetry:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
        return;
    }

    // API: Fetch configuration for a specific device directly from DB
    if (urlPath === '/api/device/config' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const result = await pool.query(
                `SELECT tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url, api_url, motor1_rate, motor2_rate, pump_threshold, alert_min, alert_max
                 FROM w_device_config 
                 WHERE device_id = $1`,
                [deviceId]
            );
            
            if (result.rows.length > 0) {
                const config = result.rows[0];
                sendJSON(res, { 
                    success: true, 
                    config: {
                        tank_height: parseFloat(config.tank_height),
                        sensor_height: parseFloat(config.sensor_height),
                        tank_diameter: parseFloat(config.tank_diameter),
                        num_tanks: parseInt(config.num_tanks, 10),
                        telemetry_interval: parseInt(config.telemetry_interval, 10),
                        gsm_numbers: config.gsm_numbers || '',
                        ota_url: config.ota_url || '',
                        api_url: config.api_url || '',
                        motor1_rate: parseFloat(config.motor1_rate || 1000.0),
                        motor2_rate: parseFloat(config.motor2_rate || 5000.0),
                        pump_threshold: parseFloat(config.pump_threshold || 2500.0),
                        alert_min: parseFloat(config.alert_min !== null ? config.alert_min : 20.0),
                        alert_max: parseFloat(config.alert_max !== null ? config.alert_max : 90.0)
                    }
                });
            } else {
                // Return default values if config is not created yet
                sendJSON(res, { 
                    success: true, 
                    config: {
                        tank_height: 200.0,
                        sensor_height: 45.0,
                        tank_diameter: 228.0,
                        num_tanks: 1,
                        telemetry_interval: 15,
                        gsm_numbers: '',
                        ota_url: '',
                        api_url: '',
                        motor1_rate: 1000.0,
                        motor2_rate: 5000.0,
                        pump_threshold: 2500.0,
                        alert_min: 20.0,
                        alert_max: 90.0
                    }
                });
            }
        } catch (err) {
            console.error('[API Error] GET /api/device/config:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Save configuration for a specific device (OTA, height, diameter, interval, etc.)
    if (urlPath === '/api/device/config' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => {
            body += chunk.toString();
        });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { device_id, tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url, api_url, motor1_rate, motor2_rate, pump_threshold, alert_min, alert_max } = data;

                if (!device_id) {
                    sendJSON(res, { success: false, error: 'device_id is required' }, 400);
                    return;
                }

                // Register device in memory set
                registeredDevices.add(device_id);

                // Construct dynamic update query based on provided fields
                const fields = [];
                const values = [];
                let paramIndex = 1;

                if (tank_height !== undefined) {
                    fields.push(`tank_height = $${paramIndex++}`);
                    values.push(parseFloat(tank_height));
                }
                if (sensor_height !== undefined) {
                    fields.push(`sensor_height = $${paramIndex++}`);
                    values.push(parseFloat(sensor_height));
                }
                if (tank_diameter !== undefined) {
                    fields.push(`tank_diameter = $${paramIndex++}`);
                    values.push(parseFloat(tank_diameter));
                }
                if (num_tanks !== undefined) {
                    fields.push(`num_tanks = $${paramIndex++}`);
                    values.push(parseInt(num_tanks, 10));
                }
                if (telemetry_interval !== undefined) {
                    fields.push(`telemetry_interval = $${paramIndex++}`);
                    values.push(parseInt(telemetry_interval, 10));
                }
                if (gsm_numbers !== undefined) {
                    fields.push(`gsm_numbers = $${paramIndex++}`);
                    values.push(gsm_numbers.toString().trim());
                }
                if (ota_url !== undefined) {
                    fields.push(`ota_url = $${paramIndex++}`);
                    values.push(ota_url.toString().trim());
                }
                if (api_url !== undefined) {
                    fields.push(`api_url = $${paramIndex++}`);
                    values.push(api_url.toString().trim());
                }
                if (motor1_rate !== undefined) {
                    fields.push(`motor1_rate = $${paramIndex++}`);
                    values.push(parseFloat(motor1_rate));
                }
                if (motor2_rate !== undefined) {
                    fields.push(`motor2_rate = $${paramIndex++}`);
                    values.push(parseFloat(motor2_rate));
                }
                if (pump_threshold !== undefined) {
                    fields.push(`pump_threshold = $${paramIndex++}`);
                    values.push(parseFloat(pump_threshold));
                }
                if (alert_min !== undefined) {
                    fields.push(`alert_min = $${paramIndex++}`);
                    values.push(parseFloat(alert_min));
                }
                if (alert_max !== undefined) {
                    fields.push(`alert_max = $${paramIndex++}`);
                    values.push(parseFloat(alert_max));
                }

                if (fields.length === 0) {
                    sendJSON(res, { success: false, error: 'No fields to update' }, 400);
                    return;
                }

                values.push(device_id);
                const queryStr = `
                    INSERT INTO w_device_config (device_id, ${fields.map(f => f.split(' = ')[0]).join(', ')})
                    VALUES ($${paramIndex}, ${fields.map((_, i) => `$${i + 1}`).join(', ')})
                    ON CONFLICT (device_id) 
                    DO UPDATE SET ${fields.join(', ')}, updated_at = CURRENT_TIMESTAMP
                `;

                // Run query
                await pool.query(queryStr, values);

                // Instantly sync settings and OTA command with the physical device via MQTT
                const syncPromises = [];
                if (tank_height !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/height`, tank_height, true));
                if (sensor_height !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/sensor_height`, sensor_height, true));
                if (tank_diameter !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/diameter`, tank_diameter, true));
                if (num_tanks !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/tanks`, num_tanks, true));
                if (telemetry_interval !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/interval`, telemetry_interval, true));
                if (gsm_numbers !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/gsm_numbers`, gsm_numbers.toString().trim(), true));
                if (api_url !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/api_url`, api_url.toString().trim(), true));
                if (motor1_rate !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/motor1_rate`, motor1_rate, true));
                if (motor2_rate !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/motor2_rate`, motor2_rate, true));
                if (pump_threshold !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/pump_threshold`, pump_threshold, true));
                if (alert_min !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/alert_min`, alert_min, true));
                if (alert_max !== undefined) syncPromises.push(publishMqttMessage(`${device_id}/config/alert_max`, alert_max, true));

                // Publish OTA trigger immediately to initiate Over-the-Air firmware flasher
                if (ota_url !== undefined && ota_url.toString().trim().length > 0) {
                    syncPromises.push(publishMqttMessage(`${device_id}/cmd/ota`, ota_url.toString().trim(), false));
                }

                if (syncPromises.length > 0) {
                    await Promise.all(syncPromises);
                }

                console.log(`[HTTP API Log] Successfully updated configuration for device ${device_id}:`, data);
                sendJSON(res, { success: true, message: 'Device configuration saved successfully.' });
            } catch (err) {
                console.error('[API Error] POST /api/device/config:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
        return;
    }

    // API: Fetch latest/current telemetry for a specific device directly from DB
    if (urlPath === '/api/telemetry' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const result = await pool.query(
                `SELECT id, device_id, level, volume, data_usage, version, timestamp 
                 FROM w_telemetry 
                 WHERE device_id = $1 
                 ORDER BY timestamp DESC LIMIT 1`,
                [deviceId]
            );
            
            if (result.rows.length > 0) {
                sendJSON(res, { success: true, telemetry: result.rows[0] });
            } else {
                // Fallback default response if database has no records yet
                sendJSON(res, { 
                    success: true, 
                    telemetry: {
                        device_id: deviceId,
                        level: 0.0,
                        volume: 0.0,
                        data_usage: 0,
                        version: '1.6',
                        timestamp: new Date().toISOString()
                    }
                });
            }
        } catch (err) {
            console.error('[API Error] GET /api/telemetry:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: List unique device IDs
    if (urlPath === '/api/devices' && req.method === 'GET') {
        try {
            const result = await pool.query(
                `SELECT DISTINCT device_id FROM w_telemetry ORDER BY device_id ASC`
            );
            const devices = result.rows.map(row => row.device_id);
            // Default list if database is empty
            if (devices.length === 0) {
                devices.push('mytank123');
            }
            sendJSON(res, { success: true, devices });
        } catch (err) {
            console.error('[API Error] /api/devices:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Fetch historical level/volume data
    if (urlPath === '/api/history' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const endDate = reqUrl.searchParams.get('end_date') || new Date().toISOString();
            // Default start date is 7 days ago
            const startDate = reqUrl.searchParams.get('start_date') || new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
            let tz = reqUrl.searchParams.get('tz') || 'UTC';
            if (!/^[a-zA-Z0-9_\-\/]+$/.test(tz)) {
                tz = 'UTC';
            }

            const resolution = reqUrl.searchParams.get('resolution') || '30m';

            // Fetch downsampled hourly or high-res granular data to keep charts highly performant with timezone-aware grouping
            let queryStr = `
                SELECT 
                    (date_trunc('hour', timestamp AT TIME ZONE $4) AT TIME ZONE $4) as hour_timestamp,
                    ROUND(AVG(level)::numeric, 1) as avg_level,
                    ROUND(AVG(volume)::numeric, 1) as avg_volume
                 FROM w_telemetry
                 WHERE device_id = $1 AND timestamp >= $2 AND timestamp <= $3
                 GROUP BY hour_timestamp
                 ORDER BY hour_timestamp ASC
            `;

            let intervalMinutes = 0;
            if (resolution === '30m') intervalMinutes = 30;
            else if (resolution === '15m') intervalMinutes = 15;
            else if (resolution === '10m') intervalMinutes = 10;
            else if (resolution === '5m') intervalMinutes = 5;

            if (intervalMinutes > 0) {
                queryStr = `
                    SELECT 
                        (date_trunc('hour', timestamp AT TIME ZONE $4) + 
                         (EXTRACT(minute FROM timestamp AT TIME ZONE $4)::int / ${intervalMinutes} * ${intervalMinutes}) * interval '1 minute') AT TIME ZONE $4 as hour_timestamp,
                        ROUND(AVG(level)::numeric, 1) as avg_level,
                        ROUND(AVG(volume)::numeric, 1) as avg_volume
                     FROM w_telemetry
                     WHERE device_id = $1 AND timestamp >= $2 AND timestamp <= $3
                     GROUP BY hour_timestamp
                     ORDER BY hour_timestamp ASC
                `;
            }

            const result = await pool.query(queryStr, [deviceId, startDate, endDate, tz]);

            sendJSON(res, { success: true, data: result.rows });
        } catch (err) {
            console.error('[API Error] /api/history:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Fetch daily water consumption and data usage
    if (urlPath === '/api/usage' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const endDate = reqUrl.searchParams.get('end_date') || new Date().toISOString();
            // Default start date is 30 days ago
            const startDate = reqUrl.searchParams.get('start_date') || new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

            const method = reqUrl.searchParams.get('method') || 'method_1';
            const motor1_rate = parseFloat(reqUrl.searchParams.get('motor1_rate')) || 1000.0;
            const motor2_rate = parseFloat(reqUrl.searchParams.get('motor2_rate')) || 5000.0;
            const threshold = parseFloat(reqUrl.searchParams.get('threshold')) || 2500.0;
            let tz = reqUrl.searchParams.get('tz') || 'UTC';
            if (!/^[a-zA-Z0-9_\-\/]+$/.test(tz)) {
                tz = 'UTC';
            }

            // Calculate daily usage using the robust window function or Method 1 flow-rate correction
            const result = await pool.query(
                `WITH ordered_telemetry AS (
                    SELECT 
                        timestamp,
                        volume,
                        data_usage,
                        AVG(volume) OVER (
                            ORDER BY timestamp 
                            ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                        ) as smoothed_volume,
                        LAG(data_usage) OVER (ORDER BY timestamp) as prev_data_usage,
                        LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                    FROM w_telemetry
                    WHERE device_id = $1 AND timestamp >= $2 AND timestamp <= $3
                ),
                smoothed_deltas AS (
                    SELECT
                        timestamp,
                        smoothed_volume,
                        data_usage,
                        prev_data_usage,
                        LAG(smoothed_volume) OVER (ORDER BY timestamp) as prev_smoothed_volume,
                        prev_timestamp
                    FROM ordered_telemetry
                ),
                deltas AS (
                    SELECT
                        (timestamp AT TIME ZONE $8)::date as usage_date,
                        CASE 
                            -- Method 1: Flow Rate Correction
                            WHEN $4 = 'method_1' THEN
                                CASE
                                    WHEN prev_timestamp IS NOT NULL AND prev_smoothed_volume IS NOT NULL AND timestamp > prev_timestamp THEN
                                        CASE
                                            -- If water level is rising (smoothed_volume > prev_smoothed_volume)
                                            WHEN smoothed_volume > prev_smoothed_volume THEN
                                                CASE
                                                    -- Motor 2 (2-inch)
                                                    WHEN ((smoothed_volume - prev_smoothed_volume) / (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) >= $7 THEN
                                                        GREATEST(0, ($6 * (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) - (smoothed_volume - prev_smoothed_volume))
                                                    -- Motor 1 (1-inch)
                                                    ELSE
                                                        GREATEST(0, ($5 * (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) - (smoothed_volume - prev_smoothed_volume))
                                                END
                                            -- If water level is falling or stable
                                            ELSE
                                                GREATEST(0, prev_smoothed_volume - smoothed_volume)
                                        END
                                    ELSE 0
                                END
                            -- Method 0: Standard Net Delta Only
                            ELSE
                                CASE 
                                    WHEN prev_smoothed_volume IS NOT NULL AND prev_smoothed_volume > smoothed_volume THEN prev_smoothed_volume - smoothed_volume 
                                    ELSE 0 
                                END
                        END as water_outflow,
                        CASE
                            WHEN prev_data_usage IS NOT NULL AND data_usage >= prev_data_usage THEN data_usage - prev_data_usage
                            WHEN prev_data_usage IS NOT NULL AND data_usage < prev_data_usage THEN
                                CASE
                                    WHEN data_usage < (prev_data_usage / 2) OR data_usage < 5000 THEN data_usage
                                    ELSE 0
                                END
                            ELSE 0
                        END as data_increment
                    FROM smoothed_deltas
                )
                SELECT 
                    usage_date::text as usage_date,
                    ROUND(SUM(water_outflow)::numeric, 1) as daily_water_used,
                    ROUND((SUM(data_increment) / 1024.0)::numeric, 2) as daily_data_used_kb
                FROM deltas
                GROUP BY usage_date
                ORDER BY usage_date ASC`,
                [deviceId, startDate, endDate, method, motor1_rate, motor2_rate, threshold, tz]
            );

            sendJSON(res, { success: true, data: result.rows });
        } catch (err) {
            console.error('[API Error] /api/usage:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }



    // API: Clear historical telemetry data
    if (urlPath === '/api/clear' && req.method === 'POST') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id');
            if (deviceId && deviceId !== 'all') {
                await pool.query('DELETE FROM w_telemetry WHERE device_id = $1', [deviceId]);
                sendJSON(res, { success: true, message: `Successfully cleared telemetry data for device: ${deviceId}` });
            } else {
                await pool.query('DELETE FROM w_telemetry');
                sendJSON(res, { success: true, message: 'Successfully cleared all historical telemetry data.' });
            }
        } catch (err) {
            console.error('[API Error] /api/clear:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Get SMS gateway settings
    if (urlPath === '/api/sms/config' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const result = await pool.query(
                `SELECT sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, sms_recipient_numbers, sms_alert_enabled, alert_min, alert_max, sms_msg_low, sms_msg_high, sms_msg_normal, timezone_offset, recovery_margin
                 FROM w_device_config WHERE device_id = $1`,
                [deviceId]
            );
            if (result.rows.length > 0) {
                const config = result.rows[0];
                // Provide defaults if null
                config.sms_msg_low = config.sms_msg_low || '⚠️ ALERT: Water level is critically LOW at [Percent]%! (Below [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
                config.sms_msg_high = config.sms_msg_high || '⚠️ ALERT: Water level is critically HIGH at [Percent]%! (Above [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
                config.sms_msg_normal = config.sms_msg_normal || 'ℹ️ RECOVERY: Water level is back to NORMAL range: [Percent]%. Device ID: [Device]. Time: [Timestamp]';
                config.timezone_offset = config.timezone_offset !== undefined && config.timezone_offset !== null ? config.timezone_offset : 0;
                config.recovery_margin = config.recovery_margin !== undefined && config.recovery_margin !== null ? parseFloat(config.recovery_margin) : 5.0;
                sendJSON(res, { success: true, config });
            } else {
                sendJSON(res, {
                    success: true,
                    config: {
                        sms_api_mode: 'v3',
                        sms_oauth_endpoint: 'https://app.text.lk/api/v3/',
                        sms_http_endpoint: 'https://app.text.lk/api/http/',
                        sms_api_token: '5812|zSz889GfK4tAKEJO3PaYaPOyw3kUW86LRgLbu7JSd908c821',
                        sms_sender_id: 'TextLK',
                        sms_recipient_numbers: '',
                        sms_alert_enabled: false,
                        alert_min: 20.0,
                        alert_max: 90.0,
                        sms_msg_low: '⚠️ ALERT: Water level is critically LOW at [Percent]%! (Below [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]',
                        sms_msg_high: '⚠️ ALERT: Water level is critically HIGH at [Percent]%! (Above [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]',
                        sms_msg_normal: 'ℹ️ RECOVERY: Water level is back to NORMAL range: [Percent]%. Device ID: [Device]. Time: [Timestamp]',
                        timezone_offset: 0,
                        recovery_margin: 5.0
                    }
                });
            }
        } catch (err) {
            console.error('[API Error] GET /api/sms/config:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Save SMS gateway settings
    if (urlPath === '/api/sms/config' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk.toString());
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { device_id, sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, sms_recipient_numbers, sms_alert_enabled, sms_msg_low, sms_msg_high, sms_msg_normal, timezone_offset, recovery_margin } = data;
                
                if (!device_id) {
                    sendJSON(res, { success: false, error: 'device_id is required' }, 400);
                    return;
                }

                const smsAlertEnabled = sms_alert_enabled === true || sms_alert_enabled === 'true';
                const tzOffset = timezone_offset !== undefined && timezone_offset !== null ? parseInt(timezone_offset, 10) : 0;
                const margin = recovery_margin !== undefined && recovery_margin !== null ? parseFloat(recovery_margin) : 5.0;

                await pool.query(
                    `INSERT INTO w_device_config (
                        device_id, sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, sms_recipient_numbers, sms_alert_enabled, sms_msg_low, sms_msg_high, sms_msg_normal, timezone_offset, recovery_margin
                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                     ON CONFLICT (device_id) DO UPDATE SET
                        sms_api_mode = EXCLUDED.sms_api_mode,
                        sms_oauth_endpoint = EXCLUDED.sms_oauth_endpoint,
                        sms_http_endpoint = EXCLUDED.sms_http_endpoint,
                        sms_api_token = EXCLUDED.sms_api_token,
                        sms_sender_id = EXCLUDED.sms_sender_id,
                        sms_recipient_numbers = EXCLUDED.sms_recipient_numbers,
                        sms_alert_enabled = EXCLUDED.sms_alert_enabled,
                        sms_msg_low = COALESCE(EXCLUDED.sms_msg_low, w_device_config.sms_msg_low),
                        sms_msg_high = COALESCE(EXCLUDED.sms_msg_high, w_device_config.sms_msg_high),
                        sms_msg_normal = COALESCE(EXCLUDED.sms_msg_normal, w_device_config.sms_msg_normal),
                        timezone_offset = COALESCE(EXCLUDED.timezone_offset, w_device_config.timezone_offset),
                        recovery_margin = COALESCE(EXCLUDED.recovery_margin, w_device_config.recovery_margin),
                        updated_at = CURRENT_TIMESTAMP`,
                    [
                        device_id,
                        sms_api_mode || 'v3',
                        sms_oauth_endpoint || 'https://app.text.lk/api/v3/',
                        sms_http_endpoint || 'https://app.text.lk/api/http/',
                        sms_api_token || '5812|zSz889GfK4tAKEJO3PaYaPOyw3kUW86LRgLbu7JSd908c821',
                        sms_sender_id || 'TextLK',
                        sms_recipient_numbers || '',
                        smsAlertEnabled,
                        sms_msg_low || '',
                        sms_msg_high || '',
                        sms_msg_normal || '',
                        tzOffset,
                        margin
                    ]
                );

                sendJSON(res, { success: true, message: 'SMS Gateway and Alert settings saved successfully.' });
            } catch (err) {
                console.error('[API Error] POST /api/sms/config:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
        return;
    }

    // API: Get SMS Schedules
    if (urlPath === '/api/sms/schedules' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const result = await pool.query(
                `SELECT id, device_id, schedule_type, recipient_numbers, scheduled_time::text as scheduled_time, days_of_week, message_template, is_enabled, timezone_offset, condition_type, condition_value
                 FROM w_sms_schedules
                 WHERE device_id = $1
                 ORDER BY scheduled_time ASC`,
                [deviceId]
            );
            sendJSON(res, { success: true, schedules: result.rows });
        } catch (err) {
            console.error('[API Error] GET /api/sms/schedules:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Save or Update SMS Schedule
    if (urlPath === '/api/sms/schedules' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk.toString());
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { id, device_id, schedule_type, recipient_numbers, scheduled_time, days_of_week, message_template, is_enabled, timezone_offset, condition_type, condition_value } = data;

                if (!device_id || !schedule_type || !recipient_numbers || !scheduled_time) {
                    sendJSON(res, { success: false, error: 'Missing required schedule fields' }, 400);
                    return;
                }

                const isEnabled = is_enabled !== false;
                const tzOffset = timezone_offset !== undefined ? parseInt(timezone_offset, 10) : 0;
                const condType = condition_type || 'none';
                const condValue = condition_value !== undefined && condition_value !== null && condition_value !== '' ? parseFloat(condition_value) : null;

                if (id) {
                    await pool.query(
                        `UPDATE w_sms_schedules SET
                            schedule_type = $1,
                            recipient_numbers = $2,
                            scheduled_time = $3,
                            days_of_week = $4,
                            message_template = $5,
                            is_enabled = $6,
                            timezone_offset = $7,
                            condition_type = $8,
                            condition_value = $9,
                            trigger_status = 'NORMAL'
                         WHERE id = $10 AND device_id = $11`,
                        [schedule_type, recipient_numbers, scheduled_time, days_of_week || '1,2,3,4,5,6,0', message_template || '', isEnabled, tzOffset, condType, condValue, id, device_id]
                    );
                    sendJSON(res, { success: true, message: 'Schedule updated successfully.' });
                } else {
                    await pool.query(
                        `INSERT INTO w_sms_schedules (
                            device_id, schedule_type, recipient_numbers, scheduled_time, days_of_week, message_template, is_enabled, timezone_offset, condition_type, condition_value
                         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
                        [device_id, schedule_type, recipient_numbers, scheduled_time, days_of_week || '1,2,3,4,5,6,0', message_template || '', isEnabled, tzOffset, condType, condValue]
                    );
                    sendJSON(res, { success: true, message: 'Schedule created successfully.' });
                }
            } catch (err) {
                console.error('[API Error] POST /api/sms/schedules:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
        return;
    }

    // API: Delete SMS Schedule
    if (urlPath === '/api/sms/schedules' && req.method === 'DELETE') {
        try {
            const id = reqUrl.searchParams.get('id');
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            if (!id) {
                sendJSON(res, { success: false, error: 'id is required' }, 400);
                return;
            }
            await pool.query('DELETE FROM w_sms_schedules WHERE id = $1 AND device_id = $2', [id, deviceId]);
            sendJSON(res, { success: true, message: 'Schedule deleted successfully.' });
        } catch (err) {
            console.error('[API Error] DELETE /api/sms/schedules:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Send manual/test SMS
    if (urlPath === '/api/sms/send-test' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => body += chunk.toString());
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { device_id, sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, recipient, message } = data;

                if (!recipient || !message) {
                    sendJSON(res, { success: false, error: 'recipient and message are required' }, 400);
                    return;
                }

                const smsConfig = {
                    sms_api_mode: sms_api_mode || 'v3',
                    sms_oauth_endpoint: sms_oauth_endpoint || 'https://app.text.lk/api/v3/',
                    sms_http_endpoint: sms_http_endpoint || 'https://app.text.lk/api/http/',
                    sms_api_token: sms_api_token || '5812|zSz889GfK4tAKEJO3PaYaPOyw3kUW86LRgLbu7JSd908c821',
                    sms_sender_id: sms_sender_id || 'TextLK'
                };

                const result = await sendSmsMessageDirectly(smsConfig, recipient, message);
                await saveSmsLog(device_id || 'mytank123', recipient, `[Test SMS] ${message}`, result.success ? 'SUCCESS' : 'FAILED', result.success ? null : result.error);
                if (result.success) {
                    sendJSON(res, { success: true, message: 'Test SMS sent successfully!', rawResponse: result.rawResponse });
                } else {
                    sendJSON(res, { success: false, error: result.error, rawResponse: result.rawResponse }, 400);
                }
            } catch (err) {
                console.error('[API Error] POST /api/sms/send-test:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
        return;
    }

    // API: Vercel/Serverless Cron Trigger for Scheduled SMS Tasks
    if (urlPath === '/api/sms/cron' && req.method === 'GET') {
        try {
            console.log('[API SMS Cron] Manual/scheduled cron trigger received. Checking schedules...');
            const count = await runScheduleCheck();
            sendJSON(res, { success: true, checked: true, schedulesTriggered: count, message: 'Schedule checks completed successfully.' });
        } catch (err) {
            console.error('[API Error] GET /api/sms/cron:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }

    // API: Get SMS logs history
    if (urlPath === '/api/sms/logs' && req.method === 'GET') {
        try {
            const deviceId = reqUrl.searchParams.get('device_id') || 'mytank123';
            const result = await pool.query(
                `SELECT id, recipient, message, status, error_message, timestamp
                 FROM w_sms_logs
                 WHERE device_id = $1
                 ORDER BY timestamp DESC
                 LIMIT 25`,
                [deviceId]
            );
            sendJSON(res, { success: true, logs: result.rows });
        } catch (err) {
            console.error('[API Error] GET /api/sms/logs:', err);
            sendJSON(res, { success: false, error: err.message }, 500);
        }
        return;
    }



    // Static Web Server fallback for HTML/CSS/JS/TXT
    let safePath = urlPath === '/' ? 'index.html' : urlPath.substring(1);
    if (safePath.toLowerCase() === 'waterlevel.ino.bin' || safePath.toLowerCase() === 'waterlevel.bin') {
        safePath = 'waterlevel.bin';
    }
    let filePath = path.join(__dirname, safePath);
    
    fs.access(filePath, fs.constants.F_OK, (accessErr) => {
        if (accessErr) {
            // Fallback to public folder
            filePath = path.join(__dirname, 'public', safePath);
        }
        
        fs.readFile(filePath, (err, data) => {
            if (err) {
                res.writeHead(404, {'Content-Type': 'text/plain'});
                res.end('Not Found');
            } else {
                let ext = path.extname(filePath).toLowerCase();
                let mime = 'text/plain';
                if (ext === '.html') mime = 'text/html';
                else if (ext === '.js') mime = 'application/javascript';
                else if (ext === '.css') mime = 'text/css';
                else if (ext === '.svg') mime = 'image/svg+xml';
                else if (ext === '.json') mime = 'application/json';
                else if (ext === '.png') mime = 'image/png';
                else if (ext === '.bin') mime = 'application/octet-stream';
                
                res.writeHead(200, {
                    'Content-Type': mime,
                    'Content-Length': data.length
                });
                res.end(data);
            }
        });
    });

});

if (require.main === module || !process.env.VERCEL) {
    server.listen(3000, () => {
        console.log('HydroSync Server listening on port 3000');
    });
}

// --- Server-side SMS Gateway Helpers & Scheduler Daemon ---

const lastDeviceSmsStates = {};

async function saveSmsLog(deviceId, recipient, message, status, errorMessage = null) {
    try {
        await pool.query(
            `INSERT INTO w_sms_logs (device_id, recipient, message, status, error_message)
             VALUES ($1, $2, $3, $4, $5)`,
            [deviceId, recipient, message, status, errorMessage]
        );
    } catch (err) {
        console.error('[DB Error] Failed to save SMS log to w_sms_logs:', err);
    }
}

async function sendSmsMessageDirectlyRaw(config, recipient, messageText) {
    return new Promise((resolve) => {
        try {
            const mode = config.sms_api_mode || 'v3';
            let targetUrl = mode === 'v3' ? (config.sms_oauth_endpoint || 'https://app.text.lk/api/v3/') : (config.sms_http_endpoint || 'https://app.text.lk/api/http/');
            let payload = '';
            let headers = {};
            let method = 'POST';

            if (mode === 'v3') {
                if (!targetUrl.endsWith('/')) {
                    targetUrl += '/';
                }
                if (!targetUrl.includes('sms/send') && !targetUrl.includes('sms')) {
                    targetUrl += 'sms/send';
                }

                headers = {
                    'Authorization': `Bearer ${config.sms_api_token || config.api_token}`,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                };

                payload = JSON.stringify({
                    recipient: recipient,
                    sender_id: config.sms_sender_id || 'TextLK',
                    message: messageText
                });
                method = 'POST';
            } else {
                const urlObj = new URL(targetUrl);
                urlObj.searchParams.set('api_token', config.sms_api_token || config.api_token);
                urlObj.searchParams.set('recipient', recipient);
                urlObj.searchParams.set('sender_id', config.sms_sender_id || 'TextLK');
                urlObj.searchParams.set('message', messageText);
                
                targetUrl = urlObj.toString();
                method = 'GET';
                headers = {
                    'Accept': 'application/json'
                };
            }

            const parsedUrl = new URL(targetUrl);
            const httpModule = targetUrl.startsWith('https') ? require('https') : require('http');

            const options = {
                hostname: parsedUrl.hostname,
                port: parsedUrl.port || (targetUrl.startsWith('https') ? 443 : 80),
                path: parsedUrl.pathname + parsedUrl.search,
                method: method,
                headers: headers
            };

            if (method === 'POST' && payload) {
                options.headers['Content-Length'] = Buffer.byteLength(payload);
            }

            const smsReq = httpModule.request(options, (smsRes) => {
                let responseData = '';
                smsRes.on('data', (chunk) => {
                    responseData += chunk;
                });
                smsRes.on('end', () => {
                    try {
                        let isSuccess = false;
                        let errorDetail = '';

                        try {
                            const resJson = JSON.parse(responseData);
                            if (resJson.success || resJson.status === 'success' || resJson.ok || resJson.status === true || resJson.status === 'sent') {
                                isSuccess = true;
                            } else if (resJson.error || resJson.message) {
                                errorDetail = resJson.error || resJson.message;
                            } else {
                                if (resJson.message_id || resJson.id || resJson.data) {
                                    isSuccess = true;
                                } else {
                                    errorDetail = JSON.stringify(resJson);
                                }
                            }
                        } catch (e) {
                            const textLower = responseData.toLowerCase();
                            if (textLower.includes('success') || textLower.includes('ok') || textLower.includes('sent') || (smsRes.statusCode >= 200 && smsRes.statusCode < 300)) {
                                isSuccess = true;
                            } else {
                                errorDetail = responseData || `HTTP Status ${smsRes.statusCode}`;
                            }
                        }

                        if (isSuccess || (smsRes.statusCode >= 200 && smsRes.statusCode < 300)) {
                            resolve({ success: true, rawResponse: responseData });
                        } else {
                            resolve({ success: false, error: errorDetail || 'Gateway returned an error status', rawResponse: responseData });
                        }
                    } catch (e) {
                        resolve({ success: false, error: 'Failed to process gateway response' });
                    }
                });
            });

            smsReq.on('error', (err) => {
                console.error('[SMS Dispatch Error]:', err);
                resolve({ success: false, error: err.message });
            });

            if (method === 'POST' && payload) {
                smsReq.write(payload);
            }
            smsReq.end();

        } catch (err) {
            console.error('[SMS Send Error]:', err);
            resolve({ success: false, error: err.message });
        }
    });
}

async function sendSmsMessageDirectly(config, recipient, messageText) {
    let lastResult = { success: false, error: 'Unknown error' };
    const maxAttempts = 3;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            if (attempt > 1) {
                console.log(`[SMS Retry] Retrying transmission to ${recipient}. Attempt ${attempt} of ${maxAttempts}...`);
                // Wait for a brief period before retrying
                await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt - 1)));
            }
            lastResult = await sendSmsMessageDirectlyRaw(config, recipient, messageText);
            if (lastResult.success) {
                if (attempt > 1) {
                    console.log(`[SMS Retry] Transmission to ${recipient} succeeded on attempt ${attempt}.`);
                }
                return lastResult;
            }
            console.warn(`[SMS Retry] Attempt ${attempt} failed with error: ${lastResult.error || 'Gateway error'}`);
        } catch (err) {
            lastResult = { success: false, error: err.message };
            console.warn(`[SMS Retry] Attempt ${attempt} exception:`, err);
        }
    }
    return lastResult;
}

function getFormattedLocalTimestamp(offsetInMinutes) {
    const tzOffset = (offsetInMinutes !== undefined && offsetInMinutes !== null) ? parseInt(offsetInMinutes, 10) : 0;
    const now = new Date();
    const localTime = new Date(now.getTime() - (tzOffset * 60000));
    const year = localTime.getUTCFullYear();
    const month = (localTime.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = localTime.getUTCDate().toString().padStart(2, '0');
    const hours = localTime.getUTCHours().toString().padStart(2, '0');
    const minutes = localTime.getUTCMinutes().toString().padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}`;
}

async function getTodayDeviceUsage(deviceId, offsetInMinutes, deviceConfig = null) {
    try {
        const tzOffset = (offsetInMinutes !== undefined && offsetInMinutes !== null) ? parseInt(offsetInMinutes, 10) : 0;
        const now = new Date();
        const localTime = new Date(now.getTime() - (tzOffset * 60000));
        const localYear = localTime.getUTCFullYear();
        const localMonth = localTime.getUTCMonth();
        const localDay = localTime.getUTCDate();
        
        const localMidnight = new Date(Date.UTC(localYear, localMonth, localDay));
        const utcStart = new Date(localMidnight.getTime() + (tzOffset * 60000));
        const queryStart = new Date(utcStart.getTime() - 2 * 60 * 60 * 1000);

        let motor1 = 1000.0;
        let motor2 = 5000.0;
        let threshold = 2500.0;

        if (deviceConfig) {
            motor1 = parseFloat(deviceConfig.motor1_rate) || 1000.0;
            motor2 = parseFloat(deviceConfig.motor2_rate) || 5000.0;
            threshold = parseFloat(deviceConfig.pump_threshold) || 2500.0;
        } else {
            const cfgRes = await pool.query(
                `SELECT motor1_rate, motor2_rate, pump_threshold FROM w_device_config WHERE device_id = $1`,
                [deviceId]
            );
            if (cfgRes.rows.length > 0) {
                const row = cfgRes.rows[0];
                motor1 = parseFloat(row.motor1_rate) || 1000.0;
                motor2 = parseFloat(row.motor2_rate) || 5000.0;
                threshold = parseFloat(row.pump_threshold) || 2500.0;
            }
        }

        const res = await pool.query(
            `WITH ordered_telemetry AS (
                SELECT 
                    timestamp,
                    volume,
                    AVG(volume) OVER (
                        ORDER BY timestamp 
                        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
                    ) as smoothed_volume,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM w_telemetry
                WHERE device_id = $1 AND timestamp >= $2 AND timestamp <= $3
            ),
            smoothed_deltas AS (
                SELECT
                    timestamp,
                    smoothed_volume,
                    LAG(smoothed_volume) OVER (ORDER BY timestamp) as prev_smoothed_volume,
                    prev_timestamp
                FROM ordered_telemetry
            ),
            deltas AS (
                SELECT
                    timestamp,
                    CASE 
                        WHEN prev_timestamp IS NOT NULL AND prev_smoothed_volume IS NOT NULL AND timestamp > prev_timestamp THEN
                            CASE
                                WHEN smoothed_volume > prev_smoothed_volume THEN
                                    CASE
                                        WHEN ((smoothed_volume - prev_smoothed_volume) / (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) >= $6 THEN
                                            GREATEST(0, ($5 * (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) - (smoothed_volume - prev_smoothed_volume))
                                        ELSE
                                            GREATEST(0, ($4 * (EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / 3600.0)) - (smoothed_volume - prev_smoothed_volume))
                                    END
                                ELSE
                                    GREATEST(0, prev_smoothed_volume - smoothed_volume)
                            END
                        ELSE 0
                    END as water_outflow
                FROM smoothed_deltas
            )
            SELECT COALESCE(SUM(water_outflow), 0) as total_outflow
            FROM deltas
            WHERE timestamp >= $7`,
            [deviceId, queryStart.toISOString(), now.toISOString(), motor1, motor2, threshold, utcStart.toISOString()]
        );

        if (res.rows.length > 0) {
            return parseFloat(res.rows[0].total_outflow) || 0;
        }
        return 0;
    } catch (err) {
        console.error('[getTodayDeviceUsage Error]:', err);
        return 0;
    }
}

async function checkDeviceThresholdAlerts(deviceId, level, volume) {
    try {
        const configRes = await pool.query(
            `SELECT tank_height, tank_diameter, num_tanks, sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, sms_recipient_numbers, sms_alert_enabled, alert_min, alert_max, sms_msg_low, sms_msg_high, sms_msg_normal, timezone_offset, recovery_margin, motor1_rate, motor2_rate, pump_threshold, last_alert_state, last_low_alert_time, last_high_alert_time
             FROM w_device_config WHERE device_id = $1`,
            [deviceId]
        );
        if (configRes.rows.length === 0) return;
        const config = configRes.rows[0];

        // Fetch enabled schedules for this device
        const schedulesRes = await pool.query(
            `SELECT id, schedule_type, recipient_numbers, days_of_week, message_template, timezone_offset, condition_type, condition_value, trigger_status
             FROM w_sms_schedules
             WHERE device_id = $1 AND is_enabled = TRUE`,
            [deviceId]
        );

        if (schedulesRes.rows.length === 0) {
            return;
        }

        const tankHeight = parseFloat(config.tank_height) || 200.0;
        const tankDiameter = parseFloat(config.tank_diameter) || 228.0;
        const numTanks = parseInt(config.num_tanks, 10) || 1;

        let percent = 0;
        if (level !== null && level !== undefined) {
            percent = (parseFloat(level) / tankHeight) * 100;
        } else if (volume !== null && volume !== undefined) {
            const singleMaxVol = (Math.PI * Math.pow(tankDiameter / 2, 2) * tankHeight) / 1000;
            const totalMaxVol = singleMaxVol * numTanks;
            percent = (parseFloat(volume) / totalMaxVol) * 100;
        }
        percent = Math.min(Math.max(percent, 0), 100);

        const now = new Date();

        for (const schedule of schedulesRes.rows) {
            try {
                const isInstant = (schedule.condition_type === 'less_than' || schedule.condition_type === 'greater_than' || schedule.condition_type === 'low_alert' || schedule.condition_type === 'high_alert');
                if (!isInstant) {
                    // Non-instant schedules are skipped in this real-time check. They are processed by runScheduleCheck.
                    continue;
                }

            const tzOffset = schedule.timezone_offset !== null && schedule.timezone_offset !== undefined ? parseInt(schedule.timezone_offset, 10) : 0;
            const localTime = new Date(now.getTime() - (tzOffset * 60000));
            const currentDay = localTime.getUTCDay().toString();

            const days = (schedule.days_of_week || '').split(',').map(d => d.trim());
            if (!days.includes(currentDay)) {
                continue;
            }

            const triggerStatus = schedule.trigger_status || 'NORMAL';
            let conditionMet = false;
            let conditionDesc = '';
            let nextTriggerStatus = triggerStatus;
            
            const condType = schedule.condition_type;

            if (condType === 'less_than') {
                const thresholdVal = parseFloat(schedule.condition_value) || 0;
                if (triggerStatus === 'NORMAL') {
                    if (percent < thresholdVal) {
                        conditionMet = true;
                        nextTriggerStatus = 'TRIGGERED';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) < Threshold (${thresholdVal}%) - Triggers Alarm`;
                    } else {
                        conditionMet = false;
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) >= Threshold (${thresholdVal}%)`;
                    }
                } else { // TRIGGERED
                    const resetThreshold = thresholdVal + 10;
                    if (percent >= resetThreshold) {
                        nextTriggerStatus = 'NORMAL';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) >= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                    } else {
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) is still below reset threshold ${resetThreshold}%`;
                    }
                    conditionMet = false;
                }
            } else if (condType === 'greater_than') {
                const thresholdVal = parseFloat(schedule.condition_value) || 0;
                if (triggerStatus === 'NORMAL') {
                    if (percent > thresholdVal) {
                        conditionMet = true;
                        nextTriggerStatus = 'TRIGGERED';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) > Threshold (${thresholdVal}%) - Triggers Alarm`;
                    } else {
                        conditionMet = false;
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) <= Threshold (${thresholdVal}%)`;
                    }
                } else { // TRIGGERED
                    const resetThreshold = thresholdVal - 10;
                    if (percent <= resetThreshold) {
                        nextTriggerStatus = 'NORMAL';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) <= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                    } else {
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) is still above reset threshold ${resetThreshold}%`;
                    }
                    conditionMet = false;
                }
            } else if (condType === 'low_alert') {
                const alertMin = parseFloat(config.alert_min) || 20.0;
                if (triggerStatus === 'NORMAL') {
                    if (percent < alertMin) {
                        conditionMet = true;
                        nextTriggerStatus = 'TRIGGERED';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) < Low Margin (${alertMin}%) - Triggers Alarm`;
                    } else {
                        conditionMet = false;
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) >= Low Margin (${alertMin}%)`;
                    }
                } else { // TRIGGERED
                    const resetThreshold = alertMin + 10;
                    if (percent >= resetThreshold) {
                        nextTriggerStatus = 'NORMAL';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) >= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                    } else {
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) is still below reset threshold ${resetThreshold}%`;
                    }
                    conditionMet = false;
                }
            } else if (condType === 'high_alert') {
                const alertMax = parseFloat(config.alert_max) || 90.0;
                if (triggerStatus === 'NORMAL') {
                    if (percent > alertMax) {
                        conditionMet = true;
                        nextTriggerStatus = 'TRIGGERED';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) > High Margin (${alertMax}%) - Triggers Alarm`;
                    } else {
                        conditionMet = false;
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) <= High Margin (${alertMax}%)`;
                    }
                } else { // TRIGGERED
                    const resetThreshold = alertMax - 10;
                    if (percent <= resetThreshold) {
                        nextTriggerStatus = 'NORMAL';
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) <= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                    } else {
                        conditionDesc = `Water Level (${percent.toFixed(1)}%) is still above reset threshold ${resetThreshold}%`;
                    }
                    conditionMet = false;
                }
            }

            if (nextTriggerStatus !== triggerStatus) {
                await pool.query(
                    `UPDATE w_sms_schedules SET trigger_status = $1 WHERE id = $2`,
                    [nextTriggerStatus, schedule.id]
                );
                console.log(`[Instant SMS Triggers] Schedule ID ${schedule.id} trigger_status updated from ${triggerStatus} to ${nextTriggerStatus}. Reason: ${conditionDesc}`);
            }

            if (conditionMet) {
                console.log(`[Instant SMS Triggers] Triggering schedule ID ${schedule.id} (${schedule.schedule_type}) for device ${deviceId}...`);
                await pool.query('UPDATE w_sms_schedules SET last_run = CURRENT_TIMESTAMP WHERE id = $1', [schedule.id]);

                let template = schedule.message_template || '';
                let thresholdValue = '';
                
                if (condType === 'low_alert') {
                    template = template || '⚠️ ALERT: Water level is critically LOW at [Percent]%! (Below [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
                    thresholdValue = (parseFloat(config.alert_min) || 20.0).toFixed(0);
                } else if (condType === 'high_alert') {
                    template = template || '⚠️ ALERT: Water level is critically HIGH at [Percent]%! (Above [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
                    thresholdValue = (parseFloat(config.alert_max) || 90.0).toFixed(0);
                } else {
                    template = template || '⚠️ ALERT: Water level trigger alert! Current level is [Percent]% (Threshold: [Threshold]%). Device ID: [Device]. Time: [Timestamp]';
                    thresholdValue = (parseFloat(schedule.condition_value) || 0.0).toFixed(0);
                }

                const timestamp = getFormattedLocalTimestamp(tzOffset);
                const dailyUsage = await getTodayDeviceUsage(deviceId, tzOffset, config);
                let message = template
                    .replace(/\[Percent\]/g, percent.toFixed(0))
                    .replace(/\[Threshold\]/g, thresholdValue)
                    .replace(/\[Device\]/g, deviceId)
                    .replace(/\[Timestamp\]/g, timestamp)
                    .replace(/\[DailyUsage\]/g, `${Math.round(dailyUsage).toLocaleString()} L`)
                    .replace(/\[Usage\]/g, `${Math.round(dailyUsage).toLocaleString()} L`);

                const recipients = (schedule.recipient_numbers || '').split(',').map(n => n.trim()).filter(Boolean);
                for (const rec of recipients) {
                    const res = await sendSmsMessageDirectly(config, rec, message);
                    console.log(`[Instant SMS Triggers] Dispatch to ${rec}: ${res.success ? 'Success' : 'Failed (' + res.error + ')'}`);
                    await saveSmsLog(deviceId, rec, message, res.success ? 'SUCCESS' : 'FAILED', res.success ? null : res.error);
                }
            }
            } catch (innerErr) {
                console.error(`[SMS Real-Time Triggers Check Loop Error] Failed to process schedule ID ${schedule.id}:`, innerErr);
            }
        }
    } catch (err) {
        console.error('[SMS Real-Time Triggers Check Error]:', err);
    }
}

// Standalone function to execute the scheduled SMS automation checks
async function runScheduleCheck() {
    let checkedCount = 0;
    try {
        const now = new Date();

        const result = await pool.query(
            `SELECT id, device_id, schedule_type, recipient_numbers, scheduled_time::text as scheduled_time, days_of_week, message_template, last_run, timezone_offset, condition_type, condition_value, trigger_status
             FROM w_sms_schedules
             WHERE is_enabled = TRUE`
        );

        for (const schedule of result.rows) {
            try {
                const tzOffset = schedule.timezone_offset !== null && schedule.timezone_offset !== undefined ? parseInt(schedule.timezone_offset, 10) : 0;
                const localTime = new Date(now.getTime() - (tzOffset * 60000));
                const currentDay = localTime.getUTCDay().toString();

                const isInstant = (schedule.condition_type === 'less_than' || schedule.condition_type === 'greater_than' || schedule.condition_type === 'low_alert' || schedule.condition_type === 'high_alert');

                const days = (schedule.days_of_week || '').split(',').map(d => d.trim());
                if (!days.includes(currentDay)) {
                    continue;
                }

                if (!isInstant) {
                    // Scheduled time checks
                    const [schedHour, schedMin] = (schedule.scheduled_time || '00:00').split(':').map(Number);
                    
                    // Construct scheduled time today in local timezone (represented in UTC fields of shifted Date)
                    const schedLocalToday = new Date(localTime);
                    schedLocalToday.setUTCHours(schedHour, schedMin, 0, 0);

                    // Is the scheduled time in the future? If so, don't run yet.
                    if (localTime.getTime() < schedLocalToday.getTime()) {
                        continue;
                    }

                    // If the scheduled time was more than 45 minutes ago, skip it (avoid stale alerts)
                    if (localTime.getTime() - schedLocalToday.getTime() > 45 * 60 * 1000) {
                        continue;
                    }

                    // Has it already run for this scheduled occurrence?
                    if (schedule.last_run) {
                        const lastRunLocal = new Date(new Date(schedule.last_run).getTime() - (tzOffset * 60000));
                        
                        // If last_run local is on the same day as localTime, and it was run after or equal to the scheduled local time
                        const isSameDay = lastRunLocal.getUTCFullYear() === localTime.getUTCFullYear() &&
                                          lastRunLocal.getUTCMonth() === localTime.getUTCMonth() &&
                                          lastRunLocal.getUTCDate() === localTime.getUTCDate();
                        
                        if (isSameDay && lastRunLocal.getTime() >= schedLocalToday.getTime()) {
                            continue;
                        }
                    }
                }

                const configRes = await pool.query(
                    `SELECT tank_height, tank_diameter, num_tanks, sms_api_mode, sms_oauth_endpoint, sms_http_endpoint, sms_api_token, sms_sender_id, motor1_rate, motor2_rate, pump_threshold, alert_min, alert_max
                     FROM w_device_config WHERE device_id = $1`,
                    [schedule.device_id]
                );
                
                if (configRes.rows.length === 0) {
                    console.warn(`[SMS Scheduler] Configuration not found for device ${schedule.device_id}. Skipping.`);
                    continue;
                }
                const config = configRes.rows[0];

                // Evaluate condition if configured
                const condType = schedule.condition_type || 'none';
                if (condType !== 'none') {
                    const telRes = await pool.query(
                        `SELECT level FROM w_telemetry WHERE device_id = $1 ORDER BY timestamp DESC LIMIT 1`,
                        [schedule.device_id]
                    );
                    
                    let currentPct = null;
                    if (telRes.rows.length > 0) {
                        const latestLvl = parseFloat(telRes.rows[0].level) || 0;
                        const tankHeight = parseFloat(config.tank_height) || 200.0;
                        currentPct = (latestLvl / tankHeight) * 100;
                        currentPct = Math.min(Math.max(currentPct, 0), 100);
                    }

                    if (currentPct === null) {
                        console.log(`[SMS Scheduler] Schedule ID ${schedule.id} has condition ${condType} but no telemetry data is available for device ${schedule.device_id}. Skipping.`);
                        continue;
                    }

                    const triggerStatus = schedule.trigger_status || 'NORMAL';
                    let conditionMet = false;
                    let conditionDesc = '';
                    let nextTriggerStatus = triggerStatus;
                    
                    if (condType === 'less_than') {
                        const thresholdVal = parseFloat(schedule.condition_value) || 0;
                        if (triggerStatus === 'NORMAL') {
                            if (currentPct < thresholdVal) {
                                conditionMet = true;
                                nextTriggerStatus = 'TRIGGERED';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) < Threshold (${thresholdVal}%) - Triggers Alarm`;
                            } else {
                                conditionMet = false;
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) >= Threshold (${thresholdVal}%)`;
                            }
                        } else { // TRIGGERED
                            const resetThreshold = thresholdVal + 10;
                            if (currentPct >= resetThreshold) {
                                nextTriggerStatus = 'NORMAL';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) >= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                            } else {
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) is still below reset threshold ${resetThreshold}%`;
                            }
                            conditionMet = false;
                        }
                    } else if (condType === 'greater_than') {
                        const thresholdVal = parseFloat(schedule.condition_value) || 0;
                        if (triggerStatus === 'NORMAL') {
                            if (currentPct > thresholdVal) {
                                conditionMet = true;
                                nextTriggerStatus = 'TRIGGERED';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) > Threshold (${thresholdVal}%) - Triggers Alarm`;
                            } else {
                                conditionMet = false;
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) <= Threshold (${thresholdVal}%)`;
                            }
                        } else { // TRIGGERED
                            const resetThreshold = thresholdVal - 10;
                            if (currentPct <= resetThreshold) {
                                nextTriggerStatus = 'NORMAL';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) <= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                            } else {
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) is still above reset threshold ${resetThreshold}%`;
                            }
                            conditionMet = false;
                        }
                    } else if (condType === 'low_alert') {
                        const alertMin = parseFloat(config.alert_min) || 20.0;
                        if (triggerStatus === 'NORMAL') {
                            if (currentPct < alertMin) {
                                conditionMet = true;
                                nextTriggerStatus = 'TRIGGERED';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) < Low Margin (${alertMin}%) - Triggers Alarm`;
                            } else {
                                conditionMet = false;
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) >= Low Margin (${alertMin}%)`;
                            }
                        } else { // TRIGGERED
                            const resetThreshold = alertMin + 10;
                            if (currentPct >= resetThreshold) {
                                nextTriggerStatus = 'NORMAL';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) >= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                            } else {
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) is still below reset threshold ${resetThreshold}%`;
                            }
                            conditionMet = false;
                        }
                    } else if (condType === 'high_alert') {
                        const alertMax = parseFloat(config.alert_max) || 90.0;
                        if (triggerStatus === 'NORMAL') {
                            if (currentPct > alertMax) {
                                conditionMet = true;
                                nextTriggerStatus = 'TRIGGERED';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) > High Margin (${alertMax}%) - Triggers Alarm`;
                            } else {
                                conditionMet = false;
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) <= High Margin (${alertMax}%)`;
                            }
                        } else { // TRIGGERED
                            const resetThreshold = alertMax - 10;
                            if (currentPct <= resetThreshold) {
                                nextTriggerStatus = 'NORMAL';
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) <= Reset Threshold (${resetThreshold}%) - Resets Alarm`;
                            } else {
                                conditionDesc = `Water Level (${currentPct.toFixed(1)}%) is still above reset threshold ${resetThreshold}%`;
                            }
                            conditionMet = false;
                        }
                    }

                    if (nextTriggerStatus !== triggerStatus) {
                        await pool.query(
                            `UPDATE w_sms_schedules SET trigger_status = $1 WHERE id = $2`,
                            [nextTriggerStatus, schedule.id]
                        );
                        console.log(`[SMS Scheduler] Schedule ID ${schedule.id} trigger_status updated from ${triggerStatus} to ${nextTriggerStatus}. Reason: ${conditionDesc}`);
                    }

                    if (!conditionMet) {
                        if (!isInstant) {
                            console.log(`[SMS Scheduler] Schedule ID ${schedule.id} skipped because condition was not met: ${conditionDesc}`);
                            const recipients = (schedule.recipient_numbers || '').split(',').map(n => n.trim()).filter(Boolean);
                            const skipMsg = `[Automation Skipped] Condition not met: ${conditionDesc}`;
                            for (const rec of recipients) {
                                await saveSmsLog(schedule.device_id, rec, skipMsg, 'SKIPPED', `Condition not met: ${conditionDesc}`);
                            }
                        }
                        await pool.query('UPDATE w_sms_schedules SET last_run = CURRENT_TIMESTAMP WHERE id = $1', [schedule.id]);
                        continue;
                    }
                }

                console.log(`[SMS Scheduler] Triggering schedule ID ${schedule.id} (${schedule.schedule_type}) for device ${schedule.device_id}...`);
                checkedCount++;

                await pool.query('UPDATE w_sms_schedules SET last_run = CURRENT_TIMESTAMP WHERE id = $1', [schedule.id]);

                let message = '';
                
                if (schedule.schedule_type === 'status_update') {
                    const telRes = await pool.query(
                        `SELECT level, volume, timestamp FROM w_telemetry WHERE device_id = $1 ORDER BY timestamp DESC LIMIT 1`,
                        [schedule.device_id]
                    );

                    let percent = 0;
                    let volume = 0;
                    let depth = 0;
                    if (telRes.rows.length > 0) {
                        const row = telRes.rows[0];
                        depth = parseFloat(row.level) || 0;
                        volume = parseFloat(row.volume) || 0;
                        
                        const tankHeight = parseFloat(config.tank_height) || 200.0;
                        const tankDiameter = parseFloat(config.tank_diameter) || 228.0;
                        const numTanks = parseInt(config.num_tanks, 10) || 1;
                        percent = (depth / tankHeight) * 100;
                        percent = Math.min(Math.max(percent, 0), 100);
                    }

                    message = schedule.message_template || `HydroSync Status for Device [Device]: Level is [Percent], Volume is [Volume], Depth is [Depth].`;
                    const dailyUsage = await getTodayDeviceUsage(schedule.device_id, schedule.timezone_offset, config);
                    message = message
                        .replace(/\[Device\]/g, schedule.device_id)
                        .replace(/\[Percent\]/g, `${percent.toFixed(0)}%`)
                        .replace(/\[Volume\]/g, `${volume.toLocaleString()} L`)
                        .replace(/\[Depth\]/g, `${depth.toFixed(1)} cm`)
                        .replace(/\[Timestamp\]/g, getFormattedLocalTimestamp(schedule.timezone_offset))
                        .replace(/\[DailyUsage\]/g, `${Math.round(dailyUsage).toLocaleString()} L`)
                        .replace(/\[Usage\]/g, `${Math.round(dailyUsage).toLocaleString()} L`);
                } else if (schedule.schedule_type === 'motor_on') {
                    message = schedule.message_template || 'MOTOR ON';
                    await publishMqttMessage(`${schedule.device_id}/cmd/motor`, 'ON', false);
                } else if (schedule.schedule_type === 'motor_off') {
                    message = schedule.message_template || 'MOTOR OFF';
                    await publishMqttMessage(`${schedule.device_id}/cmd/motor`, 'OFF', false);
                }

                const recipients = (schedule.recipient_numbers || '').split(',').map(n => n.trim()).filter(Boolean);
                for (const rec of recipients) {
                    console.log(`[SMS Scheduler] Sending scheduled SMS to ${rec}...`);
                    const res = await sendSmsMessageDirectly(config, rec, message);
                    console.log(`[SMS Scheduler] Dispatch results: ${res.success ? 'SUCCESS' : 'FAILED: ' + res.error}`);
                    await saveSmsLog(schedule.device_id, rec, message, res.success ? 'SUCCESS' : 'FAILED', res.success ? null : res.error);
                }
            } catch (innerErr) {
                console.error(`[SMS Scheduler Error] Failed to process schedule ID ${schedule.id}:`, innerErr);
            }
        }
    } catch (err) {
        console.error('[SMS Scheduler Error]:', err);
    }
    return checkedCount;
}

async function runApiUrlBackgroundPolling() {
    try {
        // Query all device configs with a non-empty api_url
        const res = await pool.query(
            `SELECT device_id, api_url, tank_height, tank_diameter, num_tanks FROM w_device_config 
             WHERE api_url IS NOT NULL AND api_url != ''`
        );
        
        for (const config of res.rows) {
            const { device_id, api_url, tank_height, tank_diameter, num_tanks } = config;
            
            let targetUrl = api_url.trim();
            if (!targetUrl.startsWith('http://') && !targetUrl.startsWith('https://')) {
                targetUrl = 'https://' + targetUrl;
            }

            let parsedUrl;
            try {
                parsedUrl = new URL(targetUrl);
            } catch (err) {
                console.warn(`[Background API Polling] Invalid API URL configured for ${device_id}: ${api_url}`);
                continue;
            }

            // If it's just the domain or does not include /api/telemetry, we should build the correct endpoint
            if (!targetUrl.includes('/api/telemetry')) {
                // Remove trailing slash if present
                if (targetUrl.endsWith('/')) {
                    targetUrl = targetUrl.slice(0, -1);
                }
                targetUrl = `${targetUrl}/api/telemetry?device_id=${encodeURIComponent(device_id)}`;
                try {
                    parsedUrl = new URL(targetUrl);
                } catch (e) {
                    continue;
                }
            } else if (!targetUrl.includes('device_id=')) {
                // If it already has /api/telemetry but no device_id param, append it
                const separator = targetUrl.includes('?') ? '&' : '?';
                targetUrl = `${targetUrl}${separator}device_id=${encodeURIComponent(device_id)}`;
                try {
                    parsedUrl = new URL(targetUrl);
                } catch (e) {
                    continue;
                }
            }

            // Avoid self-referencing to prevent infinite loop / duplicate logging loops
            const targetHostname = parsedUrl.hostname;
            const ourHost = process.env.APP_URL ? new URL(process.env.APP_URL).hostname : null;
            if (targetHostname === 'localhost' || targetHostname === '127.0.0.1' || (ourHost && targetHostname === ourHost)) {
                continue;
            }
            
            console.log(`[Background API Polling] Querying logger API for device ${device_id} at: ${targetUrl}`);
            
            try {
                const response = await fetch(targetUrl, {
                    headers: { 'Accept': 'application/json' },
                    signal: AbortSignal.timeout(10000) // 10 seconds timeout
                });
                
                if (!response.ok) {
                    console.warn(`[Background API Polling] API returned status ${response.status} for ${device_id}`);
                    continue;
                }
                
                const contentType = response.headers.get('content-type') || '';
                if (!contentType.includes('application/json')) {
                    console.warn(`[Background API Polling] API response from ${targetUrl} was not JSON (Content-Type: ${contentType}). Skipping.`);
                    continue;
                }
                
                let data;
                try {
                    data = await response.json();
                } catch (jsonErr) {
                    console.warn(`[Background API Polling] Failed to parse JSON response from ${targetUrl}: ${jsonErr.message}`);
                    continue;
                }
                
                let level = null;
                let volume = null;
                let dataUsage = 0;
                let version = '1.6';
                
                const telObj = data.telemetry || data;
                
                if (telObj) {
                    if (telObj.level !== undefined && telObj.level !== null) {
                        level = parseFloat(telObj.level);
                    }
                    if (telObj.volume !== undefined && telObj.volume !== null) {
                        volume = parseFloat(telObj.volume);
                    } else if (level !== null) {
                        // Calculate volume based on level
                        const height = parseFloat(tank_height) || 200.0;
                        const diameter = parseFloat(tank_diameter) || 228.0;
                        const tanks = parseInt(num_tanks, 10) || 1;
                        const radius = diameter / 2.0;
                        const maxVol = (Math.PI * Math.pow(radius, 2) * height) / 1000.0;
                        const pct = level / height;
                        volume = maxVol * pct * tanks;
                    }
                    
                    if (telObj.data_usage !== undefined && telObj.data_usage !== null) {
                        dataUsage = parseInt(telObj.data_usage, 10);
                    } else if (telObj.dataUsage !== undefined && telObj.dataUsage !== null) {
                        dataUsage = parseInt(telObj.dataUsage, 10);
                    }
                    
                    if (telObj.version) {
                        version = String(telObj.version);
                    }
                }
                
                if (level !== null && !isNaN(level)) {
                    console.log(`[Background API Polling] Successfully fetched level=${level} cm, volume=${volume?.toFixed(1)} L for ${device_id}. Logging to DB...`);
                    
                    // Insert into w_telemetry
                    await pool.query(
                        `INSERT INTO w_telemetry (device_id, level, volume, data_usage, version)
                         VALUES ($1, $2, $3, $4, $5)`,
                        [device_id, level, volume, dataUsage, version]
                    );
                    
                    // Check SMS alerts
                    await checkDeviceThresholdAlerts(device_id, level, volume);
                } else {
                    console.warn(`[Background API Polling] Failed to extract valid water level from API response:`, data);
                }
            } catch (fetchErr) {
                console.error(`[Background API Polling Error] Failed to fetch from ${targetUrl} for ${device_id}:`, fetchErr.message || fetchErr);
            }
        }
    } catch (err) {
        console.error('[Background API Polling Main Error]:', err);
    }
}

// Background scheduler daemon check loop (runs every 30 seconds for self-hosted instances)
setInterval(runScheduleCheck, 30000);

// Background API polling check loop (runs every 15 seconds)
setInterval(runApiUrlBackgroundPolling, 15000);

// Graceful shutdown to instantly release database and MQTT client resources on server restarts/stops
async function gracefulShutdown(signal) {
    console.log(`[Server] Received ${signal}. Starting graceful shutdown...`);
    
    // Close HTTP server first to reject incoming requests
    if (server && server.listening) {
        server.close(() => {
            console.log('[Server] HTTP server closed.');
        });
    }

    // Close MQTT background client
    if (mqttClient) {
        try {
            mqttClient.end(true, () => {
                console.log('[MQTT] Background client disconnected.');
            });
        } catch (err) {
            console.error('[MQTT] Error disconnecting background client:', err);
        }
    }

    // Close PostgreSQL pool
    if (pool) {
        try {
            console.log('[DB] Closing database pool connections...');
            await pool.end();
            console.log('[DB] Database pool ended successfully.');
        } catch (err) {
            console.error('[DB] Error ending database pool:', err);
        }
    }

    console.log('[Server] Graceful shutdown completed.');
    process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

module.exports = server;
