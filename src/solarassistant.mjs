// SolarAssistant backup → energiflow-CSV v1
// Reads SolarAssistant's "historic data" backup (an InfluxDB 1.8 portable backup:
// a zip of *.manifest, *.meta and *.sN.tar.gz shards containing TSM files) and
// produces hourly energy rows. Pure ES module, no dependencies: works in modern
// browsers and in Node >= 18 (uses DecompressionStream, Blob, Response).
//
// Format: https://github.com/marurup/solarProductionAnalyzer/blob/main/docs/energiflow-format.md

export const VERSION = '0.1.0';

// SolarAssistant measurement → energiflow column. Hourly measurements hold the
// mean power (W) of the hour, i.e. Wh for that hour.
export const HOURLY_FIELDS = [
  { key: 'pv',      col: 'pv_kwh',                measurement: 'PV power hourly' },
  { key: 'load',    col: 'load_kwh',              measurement: 'Load power hourly' },
  { key: 'batIn',   col: 'battery_charge_kwh',    measurement: 'Battery power in hourly' },
  { key: 'batOut',  col: 'battery_discharge_kwh', measurement: 'Battery power out hourly' },
  { key: 'gridIn',  col: 'grid_import_kwh',       measurement: 'Grid power in hourly' },
  { key: 'gridOut', col: 'grid_export_kwh',       measurement: 'Grid power out hourly' },
];
// Net series used only when the in/out pair is missing (older SolarAssistant
// versions). Sign verified against in/out: grid + = import, battery + = charging.
const NET_FALLBACK = [
  { measurement: 'Grid power hourly',    pos: 'gridIn', neg: 'gridOut' },
  { measurement: 'Battery power hourly', pos: 'batIn',  neg: 'batOut' },
];
const SOC_MEASUREMENT = 'Battery state of charge';
const HOUR = 3600000;

// ---------------------------------------------------------------- containers

async function inflate(u8, format) {
  let ds;
  try { ds = new DecompressionStream(format); } catch (err) {
    // Node < 21.2 lacks 'deflate-raw' in DecompressionStream; fall back to zlib.
    if (!globalThis.process?.versions?.node) throw err;
    const zlib = await import('node:zlib');
    return new Uint8Array(format === 'gzip' ? zlib.gunzipSync(u8) : zlib.inflateRawSync(u8));
  }
  const stream = new Blob([u8]).stream().pipeThrough(ds);
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

/** Minimal zip reader (stored + deflate, no zip64). Returns [{name, data()}]. */
export function zipEntries(u8) {
  const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
  let eocd = -1;
  for (let p = u8.length - 22; p >= Math.max(0, u8.length - 22 - 65535); p--) {
    if (dv.getUint32(p, true) === 0x06054b50) { eocd = p; break; }
  }
  if (eocd < 0) throw new Error('Filen er ikke en gyldig zip-fil.');
  const count = dv.getUint16(eocd + 10, true);
  let p = dv.getUint32(eocd + 16, true);
  const td = new TextDecoder();
  const out = [];
  for (let i = 0; i < count; i++) {
    if (dv.getUint32(p, true) !== 0x02014b50) throw new Error('Zip-filens indholdsfortegnelse er beskadiget.');
    const method = dv.getUint16(p + 10, true);
    const csize = dv.getUint32(p + 20, true);
    const nlen = dv.getUint16(p + 28, true), xlen = dv.getUint16(p + 30, true), clen = dv.getUint16(p + 32, true);
    const lho = dv.getUint32(p + 42, true);
    const name = td.decode(u8.subarray(p + 46, p + 46 + nlen));
    p += 46 + nlen + xlen + clen;
    const start = lho + 30 + dv.getUint16(lho + 26, true) + dv.getUint16(lho + 28, true);
    const raw = u8.subarray(start, start + csize);
    out.push({
      name,
      data: async () => {
        if (method === 0) return raw;
        if (method === 8) return inflate(raw, 'deflate-raw');
        throw new Error(`Zip-metode ${method} understøttes ikke (${name}).`);
      },
    });
  }
  return out;
}

/** ustar reader. Yields {name, data}. */
export function* tarEntries(u8) {
  const td = new TextDecoder('latin1');
  const str = (a, b) => td.decode(u8.subarray(a, b)).replace(/\0[\s\S]*$/, '');
  for (let p = 0; p + 512 <= u8.length;) {
    const name = str(p, p + 100);
    if (!name) break;
    const prefix = str(p + 345, p + 500);
    const size = parseInt(str(p + 124, p + 136).trim() || '0', 8);
    yield { name: prefix ? prefix + '/' + name : name, data: u8.subarray(p + 512, p + 512 + size) };
    p += 512 + Math.ceil(size / 512) * 512;
  }
}

// ---------------------------------------------------------------- TSM decoding
// Reference: InfluxDB 1.8 tsdb/engine/tsm1 (timestamp.go, float.go, int.go,
// pkg/encoding/simple8b). Block = crc32(4) | type(1) | uvarint(len ts) | ts | values.

const S8B = [[240, 0], [120, 0], [60, 1], [30, 2], [20, 3], [15, 4], [12, 5], [10, 6],
  [8, 7], [7, 8], [6, 10], [5, 12], [4, 15], [3, 20], [2, 30], [1, 60]];

// Number-based (no BigInt in the hot loop). Values are packed LSB-first in the
// low 60 bits of each big-endian word; a value may straddle the 32-bit halves.
function simple8b(dv, off, end) {
  const out = [];
  for (let p = off; p + 8 <= end; p += 8) {
    const hi = dv.getUint32(p), lo = dv.getUint32(p + 4);
    const [n, bits] = S8B[hi >>> 28];
    if (bits === 0) { for (let i = 0; i < n; i++) out.push(1); continue; }
    if (bits === 60) { out.push(Number(dv.getBigUint64(p) & 0x0FFFFFFFFFFFFFFFn)); continue; }
    const h = hi & 0x0fffffff;
    const mask = 2 ** bits - 1;
    for (let i = 0, o = 0; i < n; i++, o += bits) {
      if (o + bits <= 32) out.push((lo >>> o) & mask);
      else if (o >= 32) out.push((h >>> (o - 32)) & mask);
      else { const nl = 32 - o; out.push((lo >>> o) + (h & (2 ** (bits - nl) - 1)) * 2 ** nl); }
    }
  }
  return out;
}

function uvarint(u8, p) {
  let x = 0n, s = 0n, i = p;
  for (;;) {
    const b = u8[i++];
    x |= BigInt(b & 0x7f) << s;
    if (b < 0x80) break;
    s += 7n;
  }
  return [x, i - p];
}

const unzigzagBig = v => (v >> 1n) ^ -(v & 1n);
const unzigzag = z => (z % 2 === 1 ? -(z + 1) / 2 : z / 2);
const nsToMs = ns => Number(ns / 1000000n) + Number(ns % 1000000n) / 1e6;

/** Decode a timestamp block. Returns epoch milliseconds (Numbers). */
export function decodeTimestamps(u8, dv, off, end) {
  const enc = u8[off] >> 4;
  const stepMs = 10 ** (u8[off] & 0xf) / 1e6; // scaled delta unit, in ms
  const ts = [];
  if (enc === 0) {            // uncompressed (unscaled) ns deltas
    let last = 0;
    for (let p = off + 1; p + 8 <= end; p += 8) {
      const d = nsToMs(dv.getBigUint64(p));
      last = ts.length ? last + d : d;
      ts.push(last);
    }
  } else if (enc === 1) {     // simple8b-packed scaled deltas
    let last = nsToMs(dv.getBigUint64(off + 1));
    ts.push(last);
    for (const d of simple8b(dv, off + 9, end)) { last += d * stepMs; ts.push(last); }
  } else if (enc === 2) {     // run-length: first, delta, count (count = total)
    let last = nsToMs(dv.getBigUint64(off + 1));
    let p = off + 9;
    const [delta, n] = uvarint(u8, p); p += n;
    const [count] = uvarint(u8, p);
    const step = Number(delta) * stepMs;
    ts.push(last);
    for (let i = 1; i < Number(count); i++) { last += step; ts.push(last); }
  } else {
    throw new Error('Ukendt tidsstempel-kodning ' + enc);
  }
  for (let i = 0; i < ts.length; i++) ts[i] = Math.round(ts[i]);
  return ts;
}

const UVNAN = 0x7FF8000000000001n; // end-of-stream marker written by the float encoder
const f64 = new DataView(new ArrayBuffer(8));
const toFloat = b => { f64.setBigUint64(0, b); return f64.getFloat64(0); };

export function decodeFloats(u8, off, end) {
  if (u8[off] >> 4 !== 1) throw new Error('Ukendt float-kodning ' + (u8[off] >> 4));
  let pos = (off + 1) * 8;
  const stop = end * 8;
  const bit = () => { const b = (u8[pos >> 3] >> (7 - (pos & 7))) & 1; pos++; return b; };
  const bits = n => {
    let v = 0n;
    while (n > 0) { // read up to 8 bits at a time where aligned
      const avail = 8 - (pos & 7);
      const take = Math.min(avail, n);
      const byte = u8[pos >> 3];
      const chunk = (byte >> (avail - take)) & ((1 << take) - 1);
      v = (v << BigInt(take)) | BigInt(chunk);
      pos += take; n -= take;
    }
    return v;
  };
  let val = bits(64);
  if (val === UVNAN) return [];
  const out = [toFloat(val)];
  let lead = 0, trail = 0;
  while (pos < stop) {
    if (bit() === 0) { out.push(toFloat(val)); continue; }
    if (bit() === 1) {
      lead = Number(bits(5));
      let sig = Number(bits(6));
      if (sig === 0) sig = 64;
      trail = 64 - lead - sig;
    }
    val ^= bits(64 - lead - trail) << BigInt(trail);
    if (val === UVNAN) break;
    out.push(toFloat(val));
  }
  return out;
}

export function decodeIntegers(u8, dv, off, end) {
  const enc = u8[off] >> 4;
  const out = [];
  if (enc === 0) {
    let last = 0;
    for (let p = off + 1; p + 8 <= end; p += 8) {
      const d = Number(unzigzagBig(dv.getBigUint64(p)));
      last = out.length ? last + d : d;
      out.push(last);
    }
  } else if (enc === 1) {
    let last = Number(unzigzagBig(dv.getBigUint64(off + 1)));
    out.push(last);
    for (const d of simple8b(dv, off + 9, end)) { last += unzigzag(d); out.push(last); }
  } else if (enc === 2) {     // run-length: first, delta, count (count = repeats after first)
    let last = Number(unzigzagBig(dv.getBigUint64(off + 1)));
    let p = off + 9;
    const [d, n] = uvarint(u8, p); p += n;
    const [count] = uvarint(u8, p);
    const delta = Number(unzigzagBig(d));
    out.push(last);
    for (let i = 0; i < Number(count); i++) { last += delta; out.push(last); }
  } else {
    throw new Error('Ukendt heltals-kodning ' + enc);
  }
  return out;
}

/** Split a TSM series key "measurement,tags#!~#field" (measurement escaped). */
export function parseSeriesKey(key) {
  const sep = key.lastIndexOf('#!~#');
  const series = sep < 0 ? key : key.slice(0, sep);
  const field = sep < 0 ? '' : key.slice(sep + 4);
  let m = '', i = 0;
  for (; i < series.length; i++) {
    const c = series[i];
    if (c === '\\' && i + 1 < series.length) { m += series[++i]; continue; }
    if (c === ',') break;
    m += c;
  }
  return { measurement: m, tags: series.slice(i + 1), field };
}

/**
 * Read one TSM file. `wanted(measurement, field)` selects series to decode.
 * Returns { series: Map<key,{measurement,field,ts:number[],vs:number[]}>, maxTime }.
 * maxTime (ms) comes from the index, covering all series (decoded or not).
 */
export function readTsm(u8, wanted) {
  const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
  if (u8.length < 13 || dv.getUint32(0) !== 0x16D116D1) throw new Error('Ikke en TSM-fil.');
  const td = new TextDecoder();
  const series = new Map();
  let maxTime = -Infinity;
  let p = Number(dv.getBigUint64(u8.length - 8));
  while (p < u8.length - 8) {
    const kl = dv.getUint16(p);
    const key = td.decode(u8.subarray(p + 2, p + 2 + kl));
    p += 2 + kl;
    const n = dv.getUint16(p + 1);
    p += 3;
    const blocks = [];
    for (let i = 0; i < n; i++) {
      const max = Number(dv.getBigInt64(p + 8) / 1000000n);
      if (max > maxTime) maxTime = max;
      blocks.push({ off: Number(dv.getBigUint64(p + 16)), size: dv.getUint32(p + 24) });
      p += 28;
    }
    const k = parseSeriesKey(key);
    if (!wanted(k.measurement, k.field)) continue;
    const entry = series.get(key) || { ...k, ts: [], vs: [] };
    for (const b of blocks) {
      const s = b.off + 4, e = b.off + b.size;
      const type = u8[s];
      if (type !== 0 && type !== 1) continue; // only float / integer series are used
      const [tlen, nl] = uvarint(u8, s + 1);
      const t0 = s + 1 + nl, t1 = t0 + Number(tlen);
      const ts = decodeTimestamps(u8, dv, t0, t1);
      const vs = type === 0 ? decodeFloats(u8, t1, e) : decodeIntegers(u8, dv, t1, e);
      if (vs.length !== ts.length) throw new Error(`Uventet blokstørrelse i ${k.measurement} (${ts.length}/${vs.length}).`);
      for (let i = 0; i < ts.length; i++) { entry.ts.push(ts[i]); entry.vs.push(vs[i]); }
    }
    series.set(key, entry);
  }
  return { series, maxTime };
}

// ---------------------------------------------------------------- backup → hours

/**
 * Read a SolarAssistant backup.
 * @param {Array<{name:string, data:Uint8Array|Blob|ArrayBuffer}>} files
 *        the downloaded .zip, or the extracted *.tar.gz / *.tsm files
 * @param {{soc?:boolean, onProgress?:(done:number,total:number)=>void}} opts
 */
export async function readBackup(files, { soc = true, onProgress } = {}) {
  const toU8 = async d => d instanceof Uint8Array ? d
    : d instanceof ArrayBuffer ? new Uint8Array(d)
    : new Uint8Array(await d.arrayBuffer());

  // Collect shard sources (tar.gz or bare tsm), expanding zips.
  const shards = [];
  let databases = new Set();
  for (const f of files) {
    const u8 = await toU8(f.data);
    if (/\.zip$/i.test(f.name) || (u8[0] === 0x50 && u8[1] === 0x4b)) {
      for (const z of zipEntries(u8)) {
        if (/\.tar\.gz$/i.test(z.name) || /\.tsm$/i.test(z.name)) shards.push({ name: z.name, get: z.data });
        else if (/\.manifest$/i.test(z.name)) {
          try {
            const m = JSON.parse(new TextDecoder().decode(await z.data()));
            for (const x of m.files || []) if (x.database) databases.add(x.database);
          } catch { /* manifest is informational only */ }
        }
      }
    } else if (/\.tar\.gz$/i.test(f.name) || /\.tsm$/i.test(f.name)) {
      shards.push({ name: f.name, get: async () => u8 });
    }
  }
  if (!shards.length) throw new Error('Fandt ingen InfluxDB-shards (*.tar.gz) — er det en SolarAssistant-backup?');
  // Shard order = file name order (timestamp + shard id); later data overrides earlier.
  shards.sort((a, b) => shardNo(a.name) - shardNo(b.name) || a.name.localeCompare(b.name));

  const hourlyNames = new Set([...HOURLY_FIELDS, ...NET_FALLBACK].map(f => f.measurement));
  const wanted = (m, field) => hourlyNames.has(m) || (soc && m === SOC_MEASUREMENT);
  const byMeasurement = new Map(); // measurement → field → Map<ms,value>
  const allMeasurements = new Set();
  let maxTime = -Infinity, tsmFiles = 0;

  // Unpack shards a few at a time ahead of decoding (decompression is async).
  const unpack = async s => {
    const u8 = await s.get();
    if (/\.tsm$/i.test(s.name)) return [u8];
    const tar = await inflate(u8, 'gzip');
    return [...tarEntries(tar)].filter(t => t.name.endsWith('.tsm')).map(t => t.data);
  };
  const AHEAD = 4;
  const pending = shards.slice(0, AHEAD).map(unpack);
  for (let i = 0; i < shards.length; i++) {
    const tsms = await pending[i];
    pending[i] = null;
    if (i + AHEAD < shards.length) pending.push(unpack(shards[i + AHEAD]));
    for (const t of tsms) {
      tsmFiles++;
      const r = readTsm(t, (m, f) => { allMeasurements.add(m); return wanted(m, f); });
      if (r.maxTime > maxTime) maxTime = r.maxTime;
      for (const e of r.series.values()) {
        if (!byMeasurement.has(e.measurement)) byMeasurement.set(e.measurement, new Map());
        const fields = byMeasurement.get(e.measurement);
        if (!fields.has(e.field)) fields.set(e.field, new Map());
        const pts = fields.get(e.field);
        if (e.measurement === SOC_MEASUREMENT) {
          // Raw ~10 s samples: keep only the latest sample per hour (hour → [t, v]).
          for (let j = 0; j < e.ts.length; j++) {
            const t = e.ts[j], h = t - (t % HOUR), l = pts.get(h);
            if (!l || t >= l[0]) pts.set(h, [t, e.vs[j]]);
          }
        } else {
          for (let j = 0; j < e.ts.length; j++) pts.set(e.ts[j], e.vs[j]);
        }
      }
    }
    onProgress?.(i + 1, shards.length);
  }

  // Prefer the "combined" field (sum of all inverters); else the only/first field.
  const pick = m => {
    const fields = byMeasurement.get(m);
    if (!fields) return null;
    return fields.get('combined') || [...fields.values()][0];
  };

  const hours = new Map(); // utc ms (hour start) → row
  const row = t => { let r = hours.get(t); if (!r) hours.set(t, r = {}); return r; };
  const found = {}, notes = [];
  for (const f of HOURLY_FIELDS) {
    const pts = pick(f.measurement);
    found[f.key] = !!pts;
    if (pts) for (const [t, v] of pts) row(t - (t % HOUR))[f.key] = v / 1000;
  }
  for (const f of NET_FALLBACK) {
    if (found[f.pos] || found[f.neg]) continue;
    const pts = pick(f.measurement);
    if (!pts) continue;
    found[f.pos] = found[f.neg] = true;
    notes.push(`${f.measurement}: kun netto pr. time fundet — ${f.pos}/${f.neg} er udledt af fortegnet (undervurderer timer med begge retninger).`);
    for (const [t, v] of pts) { const r = row(t - (t % HOUR)); r[f.pos] = Math.max(0, v) / 1000; r[f.neg] = Math.max(0, -v) / 1000; }
  }
  if (soc) {
    const pts = pick(SOC_MEASUREMENT);
    found.soc = !!pts;
    if (pts) for (const [h, [, v]] of pts) if (hours.has(h)) hours.get(h).soc = v;
  }

  // The hour containing the backup moment is incomplete — drop it (and anything later).
  let droppedPartial = 0;
  for (const t of [...hours.keys()]) if (t + HOUR > maxTime) { hours.delete(t); droppedPartial++; }

  const keys = [...hours.keys()].sort((a, b) => a - b);
  let gapHours = 0;
  for (let i = 1; i < keys.length; i++) gapHours += (keys[i] - keys[i - 1]) / HOUR - 1;
  const missingBattery = found.batIn ? keys.filter(k => hours.get(k).batIn === undefined).length : 0;

  if (!found.pv && !found.load) {
    throw new Error('Backuppen indeholder ikke SolarAssistants timeværdier ("PV power hourly", "Load power hourly"). Fundne målinger: '
      + [...allMeasurements].sort().slice(0, 40).join(', '));
  }

  return {
    hours, keys,
    info: {
      databases: [...databases], shards: shards.length, tsmFiles,
      first: keys[0], last: keys.at(-1), backupTime: maxTime,
      hourCount: keys.length, gapHours, missingBattery, droppedPartial,
      found, notes, measurements: [...allMeasurements].sort(),
    },
  };
}

function shardNo(name) { const m = /\.s(\d+)\.tar\.gz$/i.exec(name); return m ? +m[1] : 0; }

// ---------------------------------------------------------------- CSV

const DK_FMT = new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Europe/Copenhagen', hourCycle: 'h23',
  year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit',
});

/** ISO 8601 in Danish local time with offset, e.g. 2026-10-25T02:00:00+02:00. */
export function dkIso(ms) {
  const p = Object.fromEntries(DK_FMT.formatToParts(new Date(ms)).map(x => [x.type, x.value]));
  const local = `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
  const off = Math.round((Date.parse(local + 'Z') - ms) / 60000);
  const sign = off < 0 ? '-' : '+', a = Math.abs(off);
  return `${local}${sign}${String(Math.floor(a / 60)).padStart(2, '0')}:${String(a % 60).padStart(2, '0')}`;
}

/** Danish local date (YYYY-MM-DD) of a UTC ms timestamp. */
export const dkDate = ms => dkIso(ms).slice(0, 10);

const fmt = v => v === undefined || v === null || !Number.isFinite(v) ? ''
  : String(Math.round(v * 10000) / 10000);

/**
 * @param {{hours:Map, keys:number[], info}} backup  result of readBackup
 * @param {{from?:string, to?:string, site?:string}} opts  from/to = Danish local dates (inclusive)
 */
export function toEnergiflowCSV(backup, { from, to, site } = {}) {
  const { hours, keys, info } = backup;
  const cols = HOURLY_FIELDS.filter(f => info.found[f.key]);
  const useSoc = info.found.soc;
  const meta = [`energiflow v1`, `source=solarProductionAnalyzer-exporter-solarAssistant ${VERSION}`];
  if (site) meta.push(`site=${site.replace(/[;\r\n]/g, ' ').trim()}`);
  const lines = ['# ' + meta.join('; '),
    ['timestamp', ...cols.map(c => c.col), ...(useSoc ? ['battery_soc_pct'] : [])].join(',')];
  let n = 0;
  for (const t of keys) {
    const d = dkDate(t);
    if (from && d < from) continue;
    if (to && d > to) continue;
    const r = hours.get(t);
    lines.push([dkIso(t), ...cols.map(c => fmt(r[c.key])), ...(useSoc ? [fmt(r.soc)] : [])].join(','));
    n++;
  }
  return { csv: lines.join('\n') + '\n', rows: n };
}
