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
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
      );
    `);
    
    // Create index to optimize historical queries
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_w_telemetry_device_timestamp 
      ON w_telemetry (device_id, timestamp DESC);
    `);
    
    console.log('[DB] PostgreSQL w_telemetry table initialized successfully.');
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
