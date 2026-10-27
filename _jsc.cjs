const fs = require('fs'), vm = require('vm');
const html = fs.readFileSync(process.argv[2], 'utf8');
const re = /<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/gi;
let m, i = 0, ok = 0;
while ((m = re.exec(html))) {
  i++;
  try { new vm.Script(m[1], { filename: 'block' + i }); ok++; }
  catch (e) { console.log('SYNTAX_ERROR block ' + i + ': ' + e.message); process.exit(1); }
}
console.log('JS_OK blocks=' + i + ' ok=' + ok);
