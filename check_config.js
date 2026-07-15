const { pool } = require('./db');

async function main() {
  try {
    const res = await pool.query("SELECT * FROM w_device_config WHERE device_id = 'mytank123'");
    console.log('Config:', res.rows[0]);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}

main();
