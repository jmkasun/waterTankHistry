const { pool } = require('./db');

async function main() {
  try {
    const res = await pool.query('SELECT * FROM w_sms_logs ORDER BY timestamp DESC LIMIT 20');
    console.log('Logs:', res.rows);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}

main();
