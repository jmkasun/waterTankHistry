const fs = require('fs');
const path = require('path');

const files = ['server.js', 'api/server.js'];
files.forEach(file => {
  const filePath = path.join(__dirname, file);
  if (fs.existsSync(filePath)) {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    lines.forEach((line, idx) => {
      if (line.includes('/api/sms/cron')) {
        console.log(`${file}:${idx + 1}: ${line.trim()}`);
      }
    });
  }
});
