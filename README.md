# solarProductionAnalyzer-exporter-solarAssistant

Laver en **energiflow-CSV** ud fra en [SolarAssistant](https://solar-assistant.io)-backup. Filen indeholder sol, forbrug, batteri ind/ud, net ind/ud og batteriets ladeniveau time for time. Den kan læses ind i [solarProductionAnalyzer](https://marurup.github.io/solarProductionAnalyzer/), som så viser hvad solceller og batteri faktisk sparer, også når batteriet lades fra nettet om natten.

**Brug den her:** https://marurup.github.io/solarProductionAnalyzer-exporter-solarAssistant/

Filen læses kun i din browser og sendes ingen steder hen. Siden har ingen afhængigheder og intet byggetrin.

## Sådan gør du

1. **Hent en backup i SolarAssistant:** gå til *Configuration*, rul ned til *System* og vælg *View detail*. Vælg *Backup*, vent til den er færdig, og download zip-filen. Se også SolarAssistants [Backup/Restore-vejledning](https://solar-assistant.io/help/historic-data/backup).
2. **Lav CSV'en:** åbn siden ovenfor og træk zip-filen ind. Tjek månedstallene mod SolarAssistants *Totals*-side, vælg evt. en periode og tryk *Download*.
3. **Brug den:** læs CSV'en ind under *Inverter-data* i solarProductionAnalyzer.

Har du pakket backuppen ud, kan du i stedet vælge alle `*.tar.gz`-filerne.

## Kommandolinje

Kræver Node 18 eller nyere og ingen `npm install`.

```sh
node cli.mjs backup.zip -o energiflow.csv
node cli.mjs backup.zip --from 2026-01-01 --to 2026-06-30 -o h1.csv
node cli.mjs udpakket-mappe/ > energiflow.csv      # mappe med *.tar.gz
```

Valgmuligheder:
- `--from` og `--to` tager danske datoer, og begge er inklusive.
- `--no-soc` udelader ladeniveauet.
- `--site <navn>` sætter et navn i filens første linje.

## Hvad sker der

SolarAssistants backup er en **InfluxDB 1.8 portable backup**: en zip med `*.manifest`, `*.meta` og én `*.sN.tar.gz` pr. shard med TSM-filer. Værktøjet pakker shards ud direkte i browseren via `DecompressionStream`. Det læser TSM-formatet (Gorilla-floats, simple8b og RLE) og bruger SolarAssistants egne **timemålinger**:

| SolarAssistant-måling (felt `combined`) | energiflow-kolonne |
|---|---|
| `PV power hourly` | `pv_kwh` |
| `Load power hourly` | `load_kwh` |
| `Battery power in hourly` / `out hourly` | `battery_charge_kwh` / `battery_discharge_kwh` |
| `Grid power in hourly` / `out hourly` | `grid_import_kwh` / `grid_export_kwh` |
| `Battery state of charge` (seneste værdi i timen) | `battery_soc_pct` |

- Timemålingerne er timens gennemsnitlige effekt i W, og det svarer til Wh for timen.
- Tidsstempler skrives i dansk tid med offset, så sommertidsskiftet er entydigt.
- Den sidste, ufuldstændige time (backup-tidspunktet) udelades.
- Timer hvor en måling mangler, får en tom celle, som betyder "ukendt".
- Har en ældre SolarAssistant kun nettomålingerne (`Grid power hourly`, `Battery power hourly`), deles de efter fortegn. Siden siger så til.

**Kontrol:** på en Growatt-installation passede månedssummerne med Home Assistants energitællere (der også kommer fra SolarAssistant) inden for 0,3 % for juni og november 2026.

Formatet er beskrevet i [energiflow-CSV v1](https://github.com/marurup/solarProductionAnalyzer/blob/main/docs/energiflow-format.md).

## Udvikling

```sh
npm test                                   # syntetisk backup: alle kodninger, sommertid, huller, zip/tar.gz
SA_BACKUP=backup.zip npm test              # læser også en rigtig backup (committes aldrig)
npm run serve                              # http://localhost:8081 (ES-moduler kræver http, ikke file://)
```

Testen bygger selv en lille InfluxDB-backup med en indbygget encoder, så den kører uden rigtige data. Backups og CSV-filer er git-ignoreret.
