const https = require('https');

const paths = [
  '/api/v1/sms/send',
  '/api/v1/sms-send',
  '/api/v1/send',
  '/api/sms/send',
  '/api/sms-send',
  '/api/v1/sms',
  '/api/sms',
  '/sms/send',
  '/sms-send',
  '/api/v1/sms/send/'
];

function probePath(path) {
  return new Promise((resolve) => {
    const options = {
      hostname: 'esms.dialog.lk',
      port: 443,
      path: path,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        resolve({ path, statusCode: res.statusCode, data: data.substring(0, 100).trim() });
      });
    });
    req.on('error', (err) => {
      resolve({ path, error: err.message });
    });
    req.write(JSON.stringify({ message: 'test', destination: '94771234567', messageKey: 'test' }));
    req.end();
  });
}

async function main() {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
  console.log('Probing possible paths on esms.dialog.lk...');
  for (const path of paths) {
    const result = await probePath(path);
    if (result.error) {
      console.log(`Path: ${path} => ERROR: ${result.error}`);
    } else {
      console.log(`Path: ${result.path} => Status: ${result.statusCode} | Data: ${result.data}`);
    }
  }
}

main();
