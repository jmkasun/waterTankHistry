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
  ssl: {
    rejectUnauthorized: false
  }
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
    
    console.log('[DB] PostgreSQL w_telemetry and w_device_config tables initialized successfully.');
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
