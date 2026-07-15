const fs = require('fs');
const content = fs.readFileSync('server.js', 'utf8');
const lines = content.split('\n');
lines.forEach((line, idx) => {
  const l = line.toLowerCase();
  if (l.includes('schedule') || l.includes('interval') || l.includes('trigger') || l.includes('cron') || l.includes('sms')) {
    console.log(`${idx + 1}: ${line.trim()}`);
  }
});
