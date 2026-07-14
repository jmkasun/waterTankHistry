const { pool } = require('./db');

async function main() {
  try {
    const devices = await pool.query(`
      SELECT device_id, COUNT(*) as count, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
      FROM w_telemetry
      GROUP BY device_id
    `);
    console.log('Devices in w_telemetry:', devices.rows);

    const earliest = await pool.query(`
      SELECT timestamp, level, volume, device_id, version
      FROM w_telemetry
      ORDER BY timestamp ASC
      LIMIT 10
    `);
    console.log('Earliest 10 rows in whole table:');
    console.log(earliest.rows);

  } catch (err) {
    console.error('Error:', err);
  } finally {
    await pool.end();
  }
}

main();
