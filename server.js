const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const mqtt = require('mqtt');
const { pool, initDb } = require('./db');

// Initialize database
initDb();

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

            await pool.query(
                `INSERT INTO w_telemetry (device_id, level, volume, data_usage, version) VALUES ($1, $2, $3, $4, $5)`,
                [deviceId, levelVal, volumeVal, dataUsageVal, versionVal]
            );
            console.log(`[DB Log] Successfully saved telemetry for ${deviceId}: Level=${levelVal}cm, Volume=${volumeVal}L, DataUsage=${dataUsageVal} Bytes, Version=${versionVal}`);
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
            console.log(`[MQTT Server] Background client not connected. Initializing transient connection for ${topic}...`);
            const transientClient = mqtt.connect(mqttBroker, {
                clientId: 'hydrosync-transient-' + Math.random().toString(16).substr(2, 6),
                connectTimeout: 5000
            });

            let completed = false;
            const finish = () => {
                if (!completed) {
                    completed = true;
                    try { transientClient.end(); } catch (e) {}
                    resolve();
                }
            };

            transientClient.on('connect', () => {
                transientClient.publish(topic, payload.toString(), { qos: 1, retain: retain }, (err) => {
                    if (err) {
                        console.error(`[MQTT Transient Publish Error] Failed to publish to ${topic}:`, err);
                    } else {
                        console.log(`[MQTT Transient Publish Success] Published to ${topic}: ${payload}`);
                    }
                    finish();
                });
            });

            transientClient.on('error', (err) => {
                console.error(`[MQTT Transient Connection Error] for ${topic}:`, err);
                finish();
            });

            setTimeout(() => {
                if (!completed) {
                    console.error(`[MQTT Transient Timeout] for ${topic}`);
                    finish();
                }
            }, 5000);
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

                const levelVal = parseFloat(level);
                const volumeVal = parseFloat(volume);
                const dataUsageVal = parseInt(data_usage, 10) || 0;
                const versionVal = version ? version.toString().trim() : '1.6';

                if (isNaN(levelVal) || isNaN(volumeVal)) {
                    sendJSON(res, { success: false, error: 'Invalid level or volume' }, 400);
                    return;
                }

                // Log the telemetry point in w_telemetry
                await pool.query(
                    `INSERT INTO w_telemetry (device_id, level, volume, data_usage, version) VALUES ($1, $2, $3, $4, $5)`,
                    [device_id, levelVal, volumeVal, dataUsageVal, versionVal]
                );
                
                // Get or create device configuration
                let configRes = await pool.query(
                    `SELECT tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url
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
                        ota_url: ''
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
                        ota_url: deviceConfig.ota_url || ''
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
                `SELECT tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url, api_url, motor1_rate, motor2_rate, pump_threshold
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
                        pump_threshold: parseFloat(config.pump_threshold || 2500.0)
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
                        pump_threshold: 2500.0
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
                const { device_id, tank_height, sensor_height, tank_diameter, num_tanks, telemetry_interval, gsm_numbers, ota_url, api_url, motor1_rate, motor2_rate, pump_threshold } = data;

                if (!device_id) {
                    sendJSON(res, { success: false, error: 'device_id is required' }, 400);
                    return;
                }

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
                `SELECT id, device_id, level, volume, data_usage, version, timestamp AT TIME ZONE 'UTC' as timestamp 
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
                        timestamp::date as usage_date,
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
                            WHEN prev_data_usage IS NOT NULL AND data_usage < prev_data_usage THEN data_usage
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
                [deviceId, startDate, endDate, method, motor1_rate, motor2_rate, threshold]
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

    // API: Send SMS via FitSMS Gateway (Proxy)
    if (urlPath === '/api/send-sms' && req.method === 'POST') {
        let body = '';
        req.on('data', chunk => {
            body += chunk.toString();
        });
        req.on('end', async () => {
            try {
                const data = JSON.parse(body);
                const { api_mode, endpoint_url, api_token, recipient, sender_id, message } = data;

                if (!endpoint_url || !api_token || !recipient || !message) {
                    sendJSON(res, { success: false, error: 'Missing required fields: endpoint_url, api_token, recipient, or message' }, 400);
                    return;
                }

                const mode = api_mode || 'v4';
                let targetUrl = endpoint_url;
                let payload = '';
                let headers = {};
                let method = 'POST';

                if (mode === 'v4') {
                    // OAuth 2.0 / Bearer Token-based V4 API
                    if (!targetUrl.endsWith('/')) {
                        targetUrl += '/';
                    }
                    if (!targetUrl.includes('sms/send') && !targetUrl.includes('sms')) {
                        targetUrl += 'sms/send';
                    }

                    headers = {
                        'Authorization': `Bearer ${api_token}`,
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    };

                    payload = JSON.stringify({
                        recipient: recipient,
                        to: recipient,
                        sender_id: sender_id || '',
                        from: sender_id || '',
                        sender: sender_id || '',
                        message: message,
                        body: message
                    });
                    method = 'POST';
                } else {
                    // HTTP API Mode (using GET or POST form URL-encoded)
                    // Let's default to GET with query parameters, which is standard for HTTP endpoints
                    const urlObj = new URL(targetUrl);
                    urlObj.searchParams.set('api_token', api_token);
                    urlObj.searchParams.set('token', api_token);
                    urlObj.searchParams.set('recipient', recipient);
                    urlObj.searchParams.set('to', recipient);
                    urlObj.searchParams.set('sender_id', sender_id || '');
                    urlObj.searchParams.set('from', sender_id || '');
                    urlObj.searchParams.set('sender', sender_id || '');
                    urlObj.searchParams.set('message', message);
                    urlObj.searchParams.set('body', message);
                    
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
                                sendJSON(res, { success: true, message: 'SMS Alert dispatched successfully via FitSMS Gateway', rawResponse: responseData });
                            } else {
                                sendJSON(res, { success: false, error: errorDetail || 'FitSMS Gateway returned an error status', rawResponse: responseData }, smsRes.statusCode || 400);
                            }
                        } catch (e) {
                            sendJSON(res, { success: false, error: 'Failed to process FitSMS API response' }, 500);
                        }
                    });
                });

                smsReq.on('error', (err) => {
                    console.error('[FitSMS Gateway Proxy Error]:', err);
                    sendJSON(res, { success: false, error: err.message }, 500);
                });

                if (method === 'POST' && payload) {
                    smsReq.write(payload);
                }
                smsReq.end();

            } catch (err) {
                console.error('[API Error] /api/send-sms:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
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

module.exports = server;
