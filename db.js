const { Pool } = require('pg');

// Bypass self-signed certificate issues for testing / development databases
process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

let connectionString = process.env.DATABASE_URL || "postgres://avnadmin:AVNS_R9B2HX4X27coULjte4V@testenv-testdata.d.aivencloud.com:24652/defaultdb?sslmode=require";

// If connectionString has sslmode parameter, strip it or parse properly
if (connectionString.includes('sslmode=')) {
  connectionString = connectionString.replace(/[\?&]sslmode=[^&]*/, '');
}

const pool = new Pool({
  connectionString,
  max: 3, // Max connections allowed per process for shared PostgreSQL instance
  idleTimeoutMillis: 1000, // Close idle connections after 1 second to immediately free slots
  connectionTimeoutMillis: 4000, // Fail quickly if connection slot is unavailable
  allowExitOnIdle: true,
  ssl: {
    rejectUnauthorized: false
  }
});

// Catch pool-level unexpected errors (e.g. dropped socket, idle client error) to prevent crashes
pool.on('error', (err) => {
  console.error('[DB Pool Error] Unexpected idle client error:', err.message || err);
});

async function initDb() {
  const client = await pool.connect();
  try {
    // Create the w_telemetry table with w_ prefix as requested
    await client.query(`
      CREATE TABLE IF NOT EXISTS w_telemetry (
        id SERIAL PRIMARY KEY,
        device_id VARCHAR(100) NOT NULL,
        level NUMERIC(8, 2) NOT NULL,
        volume NUMERIC(10, 2) NOT NULL,
        data_usage BIGINT NOT NULL DEFAULT 0,
        version VARCHAR(20) DEFAULT '1.6',
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    // Ensure existing table has the version column
    await client.query(`
      ALTER TABLE w_telemetry ADD COLUMN IF NOT EXISTS version VARCHAR(20) DEFAULT '1.6';
    `);

    // Migrate any older 1.5 or empty version records to 1.6 as the new active standard
    await client.query(`
      UPDATE w_telemetry SET version = '1.6' WHERE version = '1.5' OR version IS NULL;
    `);
    
    // Create index to optimize historical queries
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_w_telemetry_device_timestamp 
      ON w_telemetry (device_id, timestamp DESC);
    `);

    // Create the w_device_config table to manage stateless, MQTT-less configurations & OTA updates
    await client.query(`
      CREATE TABLE IF NOT EXISTS w_device_config (
        device_id VARCHAR(100) PRIMARY KEY,
        tank_height NUMERIC(8, 2) NOT NULL DEFAULT 200.0,
        sensor_height NUMERIC(8, 2) NOT NULL DEFAULT 45.0,
        tank_diameter NUMERIC(8, 2) NOT NULL DEFAULT 228.0,
        num_tanks INT NOT NULL DEFAULT 1,
        telemetry_interval INT NOT NULL DEFAULT 15,
        gsm_numbers TEXT DEFAULT '',
        ota_url TEXT DEFAULT '',
        api_url TEXT DEFAULT '',
        motor1_rate NUMERIC(8, 2) NOT NULL DEFAULT 1000.0,
        motor2_rate NUMERIC(8, 2) NOT NULL DEFAULT 5000.0,
        pump_threshold NUMERIC(8, 2) NOT NULL DEFAULT 2500.0,
        alert_min NUMERIC(8, 2) NOT NULL DEFAULT 20.0,
        alert_max NUMERIC(8, 2) NOT NULL DEFAULT 90.0,
        updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Ensure w_device_config has api_url and consumption calculation columns
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS api_url TEXT DEFAULT '';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS motor1_rate NUMERIC(8, 2) NOT NULL DEFAULT 1000.0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS motor2_rate NUMERIC(8, 2) NOT NULL DEFAULT 5000.0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS pump_threshold NUMERIC(8, 2) NOT NULL DEFAULT 2500.0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS alert_min NUMERIC(8, 2) NOT NULL DEFAULT 20.0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS alert_max NUMERIC(8, 2) NOT NULL DEFAULT 90.0;
    `);

    // Add SMS Gateway & Alert configurations
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_api_mode VARCHAR(20) DEFAULT 'v3';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_oauth_endpoint TEXT DEFAULT 'https://app.text.lk/api/v3/';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_http_endpoint TEXT DEFAULT 'https://app.text.lk/api/http/';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_api_token TEXT DEFAULT '5812|zSz889GfK4tAKEJO3PaYaPOyw3kUW86LRgLbu7JSd908c821';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_sender_id VARCHAR(50) DEFAULT 'TextLK';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_recipient_numbers TEXT DEFAULT '';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_alert_enabled BOOLEAN DEFAULT FALSE;
    `);

    // Ensure custom SMS templates exist on w_device_config
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_msg_low TEXT DEFAULT '⚠️ ALERT: Water level is critically LOW at [Percent]%! (Below [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_msg_high TEXT DEFAULT '⚠️ ALERT: Water level is critically HIGH at [Percent]%! (Above [Threshold]% threshold). Device ID: [Device]. Time: [Timestamp]';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS sms_msg_normal TEXT DEFAULT 'ℹ️ RECOVERY: Water level is back to NORMAL range: [Percent]%. Device ID: [Device]. Time: [Timestamp]';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS timezone_offset INT DEFAULT 0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS recovery_margin NUMERIC(8, 2) NOT NULL DEFAULT 5.0;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS last_alert_state VARCHAR(20) DEFAULT 'NORMAL';
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS last_low_alert_time TIMESTAMPTZ;
    `);
    await client.query(`
      ALTER TABLE w_device_config ADD COLUMN IF NOT EXISTS last_high_alert_time TIMESTAMPTZ;
    `);

    // Create w_sms_schedules table
    await client.query(`
      CREATE TABLE IF NOT EXISTS w_sms_schedules (
        id SERIAL PRIMARY KEY,
        device_id VARCHAR(100) NOT NULL,
        schedule_type VARCHAR(50) NOT NULL, -- 'status_update', 'motor_on', 'motor_off'
        recipient_numbers TEXT NOT NULL,
        scheduled_time TIME NOT NULL,
        days_of_week VARCHAR(50) DEFAULT '1,2,3,4,5,6,0', -- 1=Mon, ..., 0=Sun
        message_template TEXT DEFAULT '',
        is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
        last_run TIMESTAMPTZ,
        timezone_offset INT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );
    `);

    // Ensure timezone_offset column exists in w_sms_schedules table
    await client.query(`
      ALTER TABLE w_sms_schedules ADD COLUMN IF NOT EXISTS timezone_offset INT DEFAULT 0;
    `);

    // Ensure condition columns exist on w_sms_schedules
    await client.query(`
      ALTER TABLE w_sms_schedules ADD COLUMN IF NOT EXISTS condition_type VARCHAR(50) DEFAULT 'none';
    `);
    await client.query(`
      ALTER TABLE w_sms_schedules ADD COLUMN IF NOT EXISTS condition_value NUMERIC(8, 2);
    `);
    await client.query(`
      ALTER TABLE w_sms_schedules ADD COLUMN IF NOT EXISTS trigger_status VARCHAR(50) DEFAULT 'NORMAL';
    `);

    // Create w_sms_logs table to track SMS transmission status
    await client.query(`
      CREATE TABLE IF NOT EXISTS w_sms_logs (
        id SERIAL PRIMARY KEY,
        device_id VARCHAR(100) NOT NULL,
        recipient VARCHAR(100) NOT NULL,
        message TEXT NOT NULL,
        status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED'
        error_message TEXT,
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    console.log('[DB] PostgreSQL w_telemetry, w_device_config, w_sms_schedules, and w_sms_logs tables initialized successfully.');
  } catch (err) {
    console.error('[DB] Error initializing database:', err);
  } finally {
    client.release();
  }
}

module.exports = {
  pool,
  initDb
};
