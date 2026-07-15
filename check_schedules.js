const { pool } = require('./db');

async function main() {
  try {
    const res = await pool.query('SELECT * FROM w_sms_schedules');
    console.log('Schedules:', res.rows);
  } catch (err) {
    console.error(err);
  } finally {
    await pool.end();
  }
}

main();
