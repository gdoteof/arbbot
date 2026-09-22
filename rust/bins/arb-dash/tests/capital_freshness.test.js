const fs = require('node:fs');
const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');

const html = fs.readFileSync(new URL('../src/index.html', `file://${__dirname}/`), 'utf8');
const helpers = html.match(/function capitalAge[\s\S]*?(?=function venueSummary)/)?.[0];
assert.ok(helpers, 'capital freshness helpers are present');
const context = {S:{data:{books:null}}, Infinity};
vm.runInNewContext(helpers, context);

test('cached executable marks and snapshot metadata expire on the local clock', () => {
  const books = {at:100, max_age_s:180, producer_alive:true};
  const leg = {executable_mark_at:100};
  const holdings = [leg, {executable_mark_at:100}];
  assert.equal(context.capitalAge(books, 110), 10);
  assert.equal(context.capitalAge(books, 281), 181);
  assert.equal(context.capitalFresh(books, 280), true);
  assert.equal(context.executableLegFresh(leg, books, 280), true);
  assert.equal(holdings.filter(h => context.executableLegFresh(h, books, 280)).length, 2);
  assert.equal(context.capitalFresh(books, 281), false);
  assert.equal(context.executableLegFresh(leg, books, 281), false);
  assert.equal(holdings.filter(h => context.executableLegFresh(h, books, 281)).length, 0);
});

test('future source marks fail closed', () => {
  const books = {at:100, max_age_s:180, producer_alive:true};
  const leg = {executable_mark_at:111};
  assert.equal(context.executableLegFresh(leg, books, 110), false);
});

test('capital view re-renders cached age and quote state without a fetch', () => {
  assert.match(html, /snapshot \$\{dur\(capitalAge\(b\)\)\} old/);
  assert.match(html, /const known=executableLegFresh\(p\.kalshi\)&&executableLegFresh\(p\.polymarket_us\)/);
  assert.match(html, /setInterval\(\(\) => \{ if\(page\(\)==='\/'&&S\.data\.books\) render\(\); \}, 5000\)/);
});
