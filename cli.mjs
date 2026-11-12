#!/usr/bin/env node
// Kommandolinje: SolarAssistant-backup → energiflow-CSV v1
//   node cli.mjs <backup.zip | mappe med *.tar.gz> [-o ud.csv] [--from YYYY-MM-DD] [--to YYYY-MM-DD] [--no-soc] [--site navn]
import fs from 'node:fs';
import path from 'node:path';
import { readBackup, toEnergiflowCSV, dkIso, VERSION } from './src/solarassistant.mjs';

const args = process.argv.slice(2);
const opt = { soc: true };
const inputs = [];
for (let i = 0; i < args.length; i++) {
  const a = args[i];
  if (a === '-o' || a === '--out') opt.out = args[++i];
  else if (a === '--from') opt.from = args[++i];
  else if (a === '--to') opt.to = args[++i];
  else if (a === '--site') opt.site = args[++i];
  else if (a === '--no-soc') opt.soc = false;
  else if (a === '-h' || a === '--help') { usage(); process.exit(0); }
  else if (a === '-v' || a === '--version') { console.log(VERSION); process.exit(0); }
  else inputs.push(a);
}
if (!inputs.length) { usage(); process.exit(1); }
for (const d of [opt.from, opt.to]) if (d && !/^\d{4}-\d{2}-\d{2}$/.test(d)) { console.error(`Ugyldig dato: ${d} (brug ÅÅÅÅ-MM-DD)`); process.exit(1); }

function usage() {
  console.error(`solarAssistant → energiflow ${VERSION}
Brug: node cli.mjs <backup.zip | mappe> [-o ud.csv] [--from ÅÅÅÅ-MM-DD] [--to ÅÅÅÅ-MM-DD] [--no-soc] [--site navn]
Uden -o skrives CSV til stdout.`);
}

const files = [];
for (const p of inputs) {
  if (fs.statSync(p).isDirectory()) {
    for (const f of fs.readdirSync(p)) if (/\.(tar\.gz|zip|tsm)$/i.test(f)) files.push({ name: f, data: fs.readFileSync(path.join(p, f)) });
  } else files.push({ name: path.basename(p), data: fs.readFileSync(p) });
}

const t0 = Date.now();
const backup = await readBackup(files.map(f => ({ name: f.name, data: new Uint8Array(f.data.buffer, f.data.byteOffset, f.data.length) })), {
  soc: opt.soc,
  onProgress: process.stderr.isTTY ? (d, n) => process.stderr.write(`\rLæser shard ${d}/${n}`) : undefined,
});
if (process.stderr.isTTY) process.stderr.write('\n');
const { csv, rows } = toEnergiflowCSV(backup, opt);
if (opt.out) fs.writeFileSync(opt.out, csv); else process.stdout.write(csv);

const i = backup.info;
const found = Object.entries(i.found).filter(([, v]) => v).map(([k]) => k).join(', ');
console.error([
  `Periode: ${dkIso(i.first)} → ${dkIso(i.last)} (${i.hourCount} timer, ${i.gapHours} manglende timer)`,
  `Kolonner: ${found}`,
  i.missingBattery ? `Timer uden batteridata (tomme celler): ${i.missingBattery}` : null,
  ...i.notes,
  `${rows} rækker skrevet${opt.out ? ' til ' + opt.out : ''} på ${((Date.now() - t0) / 1000).toFixed(1)} s`,
].filter(Boolean).join('\n'));
