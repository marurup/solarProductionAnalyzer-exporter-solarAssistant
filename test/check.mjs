// Test: builds a synthetic SolarAssistant/InfluxDB backup (TSM → tar.gz → zip)
// with a small encoder, then reads it back. No real (private) data needed.
//   node test/check.mjs
// Optional: SA_BACKUP=path/to/backup.zip node test/check.mjs  also reads a real backup.
import zlib from 'node:zlib';
import fs from 'node:fs';
import {
  readBackup, toEnergiflowCSV, decodeTimestamps, decodeIntegers, decodeFloats,
  parseSeriesKey, dkIso, zipEntries,
} from '../src/solarassistant.mjs';

let pass = 0, fail = 0;
const ok = (cond, msg) => { if (cond) pass++; else { fail++; console.error('FAIL:', msg); } };
const J = x => JSON.stringify(x, (k, v) => typeof v === "bigint" ? v.toString() + "n" : v);
const eq = (a, b, msg) => ok(J(a) === J(b), `${msg}: ${J(a)} !== ${J(b)}`);
const near = (a, b, msg, eps = 1e-9) => ok(Math.abs(a - b) <= eps, `${msg}: ${a} vs ${b}`);

// ------------------------------------------------------------ encoders (test-only)
const be64 = v => { const b = Buffer.alloc(8); b.writeBigUInt64BE(BigInt.asUintN(64, v)); return b; };
const uvarint = v => { const out = []; v = BigInt(v); while (v >= 0x80n) { out.push(Number(v & 0x7fn) | 0x80); v >>= 7n; } out.push(Number(v)); return Buffer.from(out); };
const zigzag = v => BigInt.asUintN(64, (v << 1n) ^ (v >> 63n));
const SEL = [[60, 1], [30, 2], [20, 3], [15, 4], [12, 5], [10, 6], [8, 7], [7, 8], [6, 10], [5, 12], [4, 15], [3, 20], [2, 30], [1, 60]];
function simple8bEncode(vals) {
  const words = [];
  for (let i = 0; i < vals.length;) {
    for (let s = 0; s < SEL.length; s++) {
      const [n, bits] = SEL[s];
      if (i + n > vals.length && s < SEL.length - 1) continue;
      const chunk = vals.slice(i, i + n);
      if (chunk.some(v => v >= (1n << BigInt(bits)))) continue;
      let w = BigInt(s + 2) << 60n;
      chunk.forEach((v, j) => { w |= v << BigInt(j * bits); });
      words.push(be64(w)); i += chunk.length; break;
    }
  }
  return Buffer.concat(words);
}
function encTs(ts, enc) { // ts: BigInt ns
  const deltas = ts.slice(1).map((t, i) => t - ts[i]);
  if (enc === 0) return Buffer.concat([Buffer.from([0x00]), be64(ts[0]), ...deltas.map(be64)]);
  // scale by 10^9 (all test timestamps are whole seconds)
  if (enc === 1) return Buffer.concat([Buffer.from([0x19]), be64(ts[0]), simple8bEncode(deltas.map(d => d / 1000000000n))]);
  if (enc === 2) return Buffer.concat([Buffer.from([0x29]), be64(ts[0]), uvarint(deltas[0] / 1000000000n), uvarint(ts.length)]);
}
function encInt(vals, enc) {
  const v = vals.map(BigInt);
  const d = v.slice(1).map((x, i) => x - v[i]);
  if (enc === 0) return Buffer.concat([Buffer.from([0x00]), be64(zigzag(v[0])), ...d.map(x => be64(zigzag(x)))]);
  if (enc === 1) return Buffer.concat([Buffer.from([0x10]), be64(zigzag(v[0])), simple8bEncode(d.map(zigzag))]);
  if (enc === 2) return Buffer.concat([Buffer.from([0x20]), be64(zigzag(v[0])), uvarint(zigzag(d[0])), uvarint(d.length)]);
}
class BitWriter {
  constructor() { this.bytes = []; this.n = 0; }
  write(v, bits) { for (let i = bits - 1; i >= 0; i--) { if ((this.n & 7) === 0) this.bytes.push(0); if ((v >> BigInt(i)) & 1n) this.bytes[this.bytes.length - 1] |= 0x80 >> (this.n & 7); this.n++; } }
}
const fbits = f => { const b = Buffer.alloc(8); b.writeDoubleBE(f); return b.readBigUInt64BE(); };
function encFloat(vals) {
  const w = new BitWriter();
  const seq = [...vals.map(fbits), 0x7FF8000000000001n];
  let prev = seq[0], lead = -1, trail = 0;
  w.write(prev, 64);
  for (const cur of seq.slice(1)) {
    const x = cur ^ prev; prev = cur;
    if (x === 0n) { w.write(0n, 1); continue; }
    w.write(1n, 1);
    let l = 0; while (l < 64 && !((x >> BigInt(63 - l)) & 1n)) l++;
    let t = 0; while (t < 64 && !((x >> BigInt(t)) & 1n)) t++;
    if (l > 31) l = 31;
    if (lead >= 0 && l >= lead && t >= trail) { w.write(0n, 1); w.write(x >> BigInt(trail), 64 - lead - trail); continue; }
    lead = l; trail = t;
    const sig = 64 - l - t;
    w.write(1n, 1); w.write(BigInt(l), 5); w.write(BigInt(sig === 64 ? 0 : sig), 6); w.write(x >> BigInt(t), sig);
  }
  return Buffer.concat([Buffer.from([0x10]), Buffer.from(w.bytes)]);
}
// TSM file: header, blocks, index, footer. series: [{key, type, ts:[BigInt], vals, tsEnc, intEnc}]
function buildTsm(series) {
  const parts = [Buffer.from([0x16, 0xD1, 0x16, 0xD1, 1])];
  let off = 5; const index = [];
  for (const s of [...series].sort((a, b) => a.key < b.key ? -1 : 1)) {
    const tsb = encTs(s.ts, s.tsEnc ?? 2);
    const vb = s.type === 0 ? encFloat(s.vals) : encInt(s.vals, s.intEnc ?? 1);
    const data = Buffer.concat([Buffer.from([s.type]), uvarint(tsb.length), tsb, vb]);
    const block = Buffer.concat([Buffer.alloc(4), data]); // crc not checked by reader
    parts.push(block);
    index.push({ s, off, size: block.length });
    off += block.length;
  }
  const idx = [];
  for (const { s, off: o, size } of index) {
    const k = Buffer.from(s.key);
    const h = Buffer.alloc(2 + k.length + 3); h.writeUInt16BE(k.length); k.copy(h, 2); h[2 + k.length] = s.type; h.writeUInt16BE(1, 3 + k.length);
    const e = Buffer.alloc(28); e.writeBigInt64BE(s.ts[0]); e.writeBigInt64BE(s.ts.at(-1), 8); e.writeBigUInt64BE(BigInt(o), 16); e.writeUInt32BE(size, 24);
    idx.push(h, e);
  }
  const footer = be64(BigInt(off));
  return Buffer.concat([...parts, ...idx, footer]);
}
function tar(files) {
  const out = [];
  for (const [name, data] of files) {
    const h = Buffer.alloc(512);
    h.write(name, 0, 'latin1'); h.write('0000644\0', 100); h.write(data.length.toString(8).padStart(11, '0') + '\0', 124); h[156] = 48; h.write('ustar\0', 257);
    out.push(h, data, Buffer.alloc((512 - (data.length % 512)) % 512));
  }
  out.push(Buffer.alloc(1024));
  return Buffer.concat(out);
}
function zip(files, method) {
  const locals = [], central = []; let off = 0;
  for (const [name, raw] of files) {
    const data = method === 8 ? zlib.deflateRawSync(raw) : raw;
    const n = Buffer.from(name);
    const lh = Buffer.alloc(30); lh.writeUInt32LE(0x04034b50); lh.writeUInt16LE(method, 8); lh.writeUInt32LE(data.length, 18); lh.writeUInt32LE(raw.length, 22); lh.writeUInt16LE(n.length, 26);
    const ch = Buffer.alloc(46); ch.writeUInt32LE(0x02014b50); ch.writeUInt16LE(method, 10); ch.writeUInt32LE(data.length, 20); ch.writeUInt32LE(raw.length, 24); ch.writeUInt16LE(n.length, 28); ch.writeUInt32LE(off, 42);
    locals.push(lh, n, data); central.push(ch, n); off += 30 + n.length + data.length;
  }
  const cd = Buffer.concat(central);
  const eocd = Buffer.alloc(22); eocd.writeUInt32LE(0x06054b50); eocd.writeUInt16LE(files.length, 8); eocd.writeUInt16LE(files.length, 10); eocd.writeUInt32LE(cd.length, 12); eocd.writeUInt32LE(off, 16);
  return Buffer.concat([...locals, cd, eocd]);
}
const u8 = b => new Uint8Array(b.buffer, b.byteOffset, b.length);

// ------------------------------------------------------------ 1. decoder unit tests
{
  const ts = [0n, 10n, 20n, 35n, 45n].map(s => 1790000000n * 1000000000n + s * 1000000000n);
  for (const enc of [0, 1]) {
    const b = encTs(ts, enc);
    eq(decodeTimestamps(b, new DataView(b.buffer, b.byteOffset, b.length), 0, b.length), ts.map(t => Number(t / 1000000n)), `timestamps enc ${enc}`);
  }
  const rle = ts.slice(0, 3);
  const b2 = encTs(rle, 2);
  eq(decodeTimestamps(b2, new DataView(b2.buffer, b2.byteOffset, b2.length), 0, b2.length), rle.map(t => Number(t / 1000000n)), 'timestamps RLE');

  const ints = [48, 49, 49, 51, 47, 100, 0, 3];
  for (const enc of [0, 1]) {
    const b = encInt(ints, enc);
    eq(decodeIntegers(b, new DataView(b.buffer, b.byteOffset, b.length), 0, b.length), ints, `integers enc ${enc}`);
  }
  const b3 = encInt([60, 61, 62, 63], 2);
  eq(decodeIntegers(b3, new DataView(b3.buffer, b3.byteOffset, b3.length), 0, b3.length), [60, 61, 62, 63], 'integers RLE');

  const floats = [0, 0, 875.4857142857145, 808.1857142857141, 808.1857142857141, -1.4277777777777778, 0.008333333333333333, 1e6, 0];
  const bf = encFloat(floats);
  eq(decodeFloats(bf, 0, bf.length), floats, 'floats gorilla');
  eq(decodeFloats(encFloat([]).subarray(0), 0, 9), [], 'empty float block');

  eq(parseSeriesKey('PV\\ power\\ hourly#!~#combined'), { measurement: 'PV power hourly', tags: '', field: 'combined' }, 'series key');
  eq(parseSeriesKey('Load\\ power,host=a\\,b#!~#inverter_0'), { measurement: 'Load power', tags: 'host=a\\,b', field: 'inverter_0' }, 'series key with tags');
  eq(dkIso(Date.parse('2026-10-25T00:00:00Z')), '2026-10-25T02:00:00+02:00', 'dkIso summer');
  eq(dkIso(Date.parse('2026-10-25T01:00:00Z')), '2026-10-25T02:00:00+01:00', 'dkIso winter (repeated 02:00)');
  eq(dkIso(Date.parse('2026-03-29T01:00:00Z')), '2026-03-29T03:00:00+02:00', 'dkIso spring forward');
}

// ------------------------------------------------------------ 2. synthetic backup
// 2026-10-24 20:00Z … 2026-10-25 05:00Z (spans the DST fall-back), hourly Wh values.
const H = 3600, NS = 1000000000n;
const start = Date.parse('2026-10-24T20:00:00Z') / 1000;
const hoursS = Array.from({ length: 10 }, (_, i) => start + i * H);
const ns = s => BigInt(s) * NS;
const val = (h, base) => base + h * 10 + 0.25;
const hourly = (name, base, skip = []) => {
  const hs = hoursS.filter((_, i) => !skip.includes(i));
  return { key: `${name.replace(/ /g, '\\ ')}#!~#combined`, type: 0, ts: hs.map(ns), vals: hs.map((_, i) => val(hoursS.indexOf(hs[i]), base)) };
};
// SOC raw every 20 min; last sample at 05:20Z → the 05:00Z hour is partial and must be dropped.
const socT = []; for (let s = start; s <= start + 9 * H + 20 * 60; s += 20 * 60) socT.push(s);
const socV = socT.map((_, i) => 20 + i);
const shardA = [hourly('PV power hourly', 0), hourly('Load power hourly', 500), hourly('Battery power in hourly', 1000, [3, 4]),
  hourly('Battery power out hourly', 2000, [3, 4]), hourly('Grid power in hourly', 3000), hourly('Grid power out hourly', 4000),
  { key: 'Battery\\ state\\ of\\ charge#!~#combined', type: 1, ts: socT.map(ns), vals: socV, tsEnc: 1, intEnc: 1 },
  { key: 'PV\\ power#!~#inverter_0', type: 1, ts: socT.map(ns), vals: socV.map(v => v * 100), tsEnc: 0, intEnc: 0 }, // raw series: not used
  { key: 'PV\\ power\\ hourly#!~#inverter_0', type: 0, ts: hoursS.map(ns), vals: hoursS.map(() => 99999) },         // 'combined' must win
];
// Split hours across two shards to test merging.
const inShard = (s, lo, hi) => {
  const keep = s.ts.map((t, i) => [t, i]).filter(([t]) => t >= ns(lo) && t < ns(hi)).map(([, i]) => i);
  return keep.length ? { ...s, ts: keep.map(i => s.ts[i]), vals: keep.map(i => s.vals[i]) } : null;
};
const mid = start + 5 * H;
const s1 = shardA.map(s => inShard(s, 0, mid)).filter(Boolean);
const s2 = shardA.map(s => inShard(s, mid, 1e12)).filter(Boolean);
const gz = tsmBuf => zlib.gzipSync(tar([['solar_assistant/autogen/1/000000001-000000001.tsm', tsmBuf]]));
const files = [
  ['20261025T052000Z.manifest', Buffer.from(JSON.stringify({ files: [{ database: 'solar_assistant', shardID: 1 }] }))],
  ['20261025T052000Z.s1.tar.gz', gz(buildTsm(s1))],
  ['20261025T052001Z.s2.tar.gz', gz(buildTsm(s2))],
];

for (const method of [0, 8]) {
  const z = zip(files, method);
  eq(zipEntries(u8(z)).map(e => e.name), files.map(f => f[0]), `zip method ${method} entries`);
  const b = await readBackup([{ name: 'backup.zip', data: u8(z) }]);
  const i = b.info;
  eq(i.databases, ['solar_assistant'], 'database from manifest');
  eq(i.shards, 2, 'two shards');
  eq(i.hourCount, 9, 'partial last hour dropped (10 → 9)');
  eq(i.droppedPartial, 1, 'droppedPartial');
  eq(i.missingBattery, 2, 'two hours without battery');
  eq(i.gapHours, 0, 'no gaps');
  const r0 = b.hours.get(start * 1000);
  near(r0.pv, val(0, 0) / 1000, 'pv kWh (combined preferred over inverter_0)');
  near(r0.gridOut, val(0, 4000) / 1000, 'gridOut kWh');
  eq(b.hours.get((start + 3 * H) * 1000).batIn, undefined, 'missing battery hour stays undefined');
  eq(r0.soc, 22, 'SOC = last sample in hour');
  eq(b.hours.get((start + 5 * H) * 1000).soc, 37, 'SOC in second shard');

  const { csv, rows } = toEnergiflowCSV(b, { site: 'Test; x' });
  const lines = csv.trim().split('\n');
  eq(lines[0], '# energiflow v1; source=solarProductionAnalyzer-exporter-solarAssistant 0.1.0; site=Test  x', 'meta line');
  eq(lines[1], 'timestamp,pv_kwh,load_kwh,battery_charge_kwh,battery_discharge_kwh,grid_import_kwh,grid_export_kwh,battery_soc_pct', 'header');
  eq(rows, 9, 'csv rows');
  eq(lines[2], '2026-10-24T22:00:00+02:00,0.0003,0.5003,1.0003,2.0003,3.0003,4.0003,22', 'first row');
  eq(lines[5].split(',').slice(3, 5), ['', ''], 'empty battery cells');
  eq(lines.filter(l => l.startsWith('2026-10-25T02:00:00')).map(l => l.slice(19, 25)), ['+02:00', '+01:00'], 'DST: 02:00 appears twice with different offsets');
  eq(toEnergiflowCSV(b, { from: '2026-10-25', to: '2026-10-25' }).rows, 7, 'date filter (Danish dates)');
}
// Extracted *.tar.gz files instead of the zip.
{
  const b = await readBackup(files.slice(1).map(([name, data]) => ({ name, data: u8(data) })), { soc: false });
  eq(b.info.hourCount, 9, 'extracted tar.gz input');
  eq(b.info.found.soc, undefined, 'soc disabled');
  ok(!toEnergiflowCSV(b).csv.includes('battery_soc_pct'), 'no soc column when disabled');
}
// Not a backup.
{
  let err = null;
  try { await readBackup([{ name: 'x.zip', data: u8(zip([['a.txt', Buffer.from('hej')]], 0)) }]); } catch (e) { err = e; }
  ok(err && /shards/.test(err.message), 'clear error for non-backup zip');
}

// ------------------------------------------------------------ 3. optional real backup
if (process.env.SA_BACKUP) {
  const buf = fs.readFileSync(process.env.SA_BACKUP);
  const t0 = Date.now();
  const b = await readBackup([{ name: 'backup.zip', data: u8(buf) }]);
  const i = b.info;
  console.log(`Ægte backup: ${dkIso(i.first)} → ${dkIso(i.last)}, ${i.hourCount} timer, ${i.gapHours} mangler, ${((Date.now() - t0) / 1000).toFixed(1)} s`);
  ok(i.found.pv && i.found.load, 'real backup has pv + load');
}

console.log(`${pass} OK, ${fail} fejl`);
process.exit(fail ? 1 : 0);
