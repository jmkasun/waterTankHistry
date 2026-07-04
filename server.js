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
        const metric = topicParts[2]; // level, volume, or data_usage
        const valStr = message.toString().trim();
        const value = parseFloat(valStr);

        if (isNaN(value)) return;

        if (!deviceCache[deviceId]) {
            deviceCache[deviceId] = {
                level: null,
                volume: null,
                data_usage: 0,
                last_insert_time: 0
            };
        }

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

        // Check if both level and volume are populated, and 15 seconds has passed since last DB entry to throttle
        if (cache.level !== null && cache.volume !== null && (now - cache.last_insert_time >= 15000)) {
            cache.last_insert_time = now;
            
            const levelVal = cache.level;
            const volumeVal = cache.volume;
            const dataUsageVal = cache.data_usage || 0;

            await pool.query(
                `INSERT INTO w_telemetry (device_id, level, volume, data_usage) VALUES ($1, $2, $3, $4)`,
                [deviceId, levelVal, volumeVal, dataUsageVal]
            );
            console.log(`[DB Log] Successfully saved telemetry for ${deviceId}: Level=${levelVal}cm, Volume=${volumeVal}L, DataUsage=${dataUsageVal} Bytes`);
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
                const { device_id, level, volume, data_usage } = data;

                if (!device_id || level === undefined || volume === undefined) {
                    sendJSON(res, { success: false, error: 'Missing required fields' }, 400);
                    return;
                }

                const levelVal = parseFloat(level);
                const volumeVal = parseFloat(volume);
                const dataUsageVal = parseInt(data_usage, 10) || 0;

                if (isNaN(levelVal) || isNaN(volumeVal)) {
                    sendJSON(res, { success: false, error: 'Invalid level or volume' }, 400);
                    return;
                }

                await pool.query(
                    `INSERT INTO w_telemetry (device_id, level, volume, data_usage) VALUES ($1, $2, $3, $4)`,
                    [device_id, levelVal, volumeVal, dataUsageVal]
                );
                
                console.log(`[HTTP API Log] Successfully saved telemetry for ${device_id}: Level=${levelVal}cm, Volume=${volumeVal}L, DataUsage=${dataUsageVal} Bytes`);
                sendJSON(res, { success: true, message: 'Telemetry logged successfully' });
            } catch (err) {
                console.error('[API Error] /api/telemetry:', err);
                sendJSON(res, { success: false, error: err.message }, 500);
            }
        });
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

            // Calculate daily usage using the robust window function
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
                        LAG(data_usage) OVER (ORDER BY timestamp) as prev_data_usage
                    FROM w_telemetry
                    WHERE device_id = $1 AND timestamp >= $2 AND timestamp <= $3
                ),
                smoothed_deltas AS (
                    SELECT
                        timestamp,
                        smoothed_volume,
                        data_usage,
                        prev_data_usage,
                        LAG(smoothed_volume) OVER (ORDER BY timestamp) as prev_smoothed_volume
                    FROM ordered_telemetry
                ),
                deltas AS (
                    SELECT
                        timestamp::date as usage_date,
                        CASE 
                            WHEN prev_smoothed_volume IS NOT NULL AND prev_smoothed_volume > smoothed_volume THEN prev_smoothed_volume - smoothed_volume 
                            ELSE 0 
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
                [deviceId, startDate, endDate]
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

    // Static Web Server fallback for HTML/CSS/JS/TXT
    const safePath = urlPath === '/' ? 'index.html' : urlPath.substring(1);
    const filePath = path.join(__dirname, safePath);
    
    fs.readFile(filePath, (err, data) => {
        if (err) {
            res.writeHead(404, {'Content-Type': 'text/plain'});
            res.end('Not Found');
        } else {
            let ext = path.extname(filePath);
            let mime = 'text/plain';
            if (ext === '.html') mime = 'text/html';
            else if (ext === '.js') mime = 'application/javascript';
            else if (ext === '.css') mime = 'text/css';
            else if (ext === '.svg') mime = 'image/svg+xml';
            else if (ext === '.json') mime = 'application/json';
            else if (ext === '.png') mime = 'image/png';
            res.writeHead(200, {'Content-Type': mime});
            res.end(data);
        }
    });

});

if (require.main === module || !process.env.VERCEL) {
    server.listen(3000, () => {
        console.log('HydroSync Server listening on port 3000');
    });
}

module.exports = server;
