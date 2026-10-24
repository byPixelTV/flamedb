# BetterAttack – vollständiges Grafana-Dashboard

`betterattack-dashboard.json` erweitert dein übergebenes Grafana-v2-Dashboard. Name `adzlbfq`, Titel `BetterAttack`, die Datasource-UID `cg24sd0kvre9sd` und die sieben ursprünglichen Spieler-Panels bleiben erhalten. Enthalten sind 62 Metriken aus deiner Liste; die SMP-Statistik ist auf deinen Wunsch entfernt. Zusätzlich gibt es drei optionale Paper-Tick-Metriken aus deinem Collector.

## Übernehmen

1. In deinem vorhandenen BetterAttack-Dashboard **Edit → Dashboard options → Settings / View all settings → JSON Model** öffnen.
2. Den bisherigen JSON-Inhalt zur Sicherung exportieren.
3. Den Inhalt von `betterattack-dashboard.json` als vollständiges v2-JSON einsetzen und speichern.
4. Die Tabs durchsehen. Die bestehende FlameDB-Datasource wird weiterverwendet; kein Plugin-Neubau nötig.

Alternativ lässt sich die Datei über den Dashboard-Import importieren. Auf derselben Instanz bezeichnet `metadata.name: adzlbfq` dein vorhandenes Dashboard. Auf einer anderen Grafana-Instanz muss die Datasource-UID in allen Query-Blöcken angepasst werden.

## Inhalt

- **Server stats:** Netzwerk-Spielerzahl, Spieler/Slots je Server, TPS/MSPT, Prozess-/Host-CPU, Chunks, Entities, Folia-Regionen, CPU-Kerne, Plugins, Uptime und Prozessstart.
- **Player Stats:** bestehende sieben Panels plus Clan-Joins und Session-Events; zusätzlich kumulative Zeitreihen.
- **JVM & Memory:** Heap/RAM, Heap-Auslastung, Heap-/Non-Heap-Speicher und Threads.
- **Garbage collection:** JVM- und Notification-Zähler, freigegebener Speicher, Dauer und die rohen kumulativen Histogramm-Buckets.
- **Worlds & Regions:** Tabellen für Welten und Regionen, TPS/MSPT/Utilization pro Region, Welt-Spieler/Chunks/Entities und Regionenzahlen pro Welt.
- **Server events:** Login-, Join-, Quit-, Chat- und Ping-Zähler seit Collector-Start.
- **Paper ticks:** Tick-Anzahl, Tick-Dauer und Tick-Buckets. Diese Metriken werden nur auf Paper, nicht auf Folia, geschrieben; dort ist eine leere Anzeige korrekt.

## Bedeutung der Zahlen

Spielereignisse werden mit `SUM` zeitlich gebündelt und danach kumulativ dargestellt. Wie bei deinem bisherigen Dashboard beginnt die Summe am Anfang des gewählten Zeitraums; frühere Historie ist kein Startwert. Die Damage-Panels summieren Schadenswerte. Bei `playtime:sessions` wird vorsichtig nur die Zahl gespeicherter Records angezeigt (**Session Events**, `COUNT`): Der gelieferte System-Collector definiert den Session-Payload nicht. Eine Spielzeitdauer oder Anzahl eindeutiger Sessions wird daraus nicht behauptet.

`system:events:*`, CPU-Sekunden, GC-Zähler, gestartete Threads und Paper-Ticks sind bereits laufende Zähler. Sie werden **nicht** noch einmal über ihre Samples aufsummiert. Prozess-/Collector-Neustarts können sie zurücksetzen. Gauges werden je Zeit-Bucket gemittelt und im Serververgleich getrennt dargestellt. Stat- und Tabellenpanels verwenden den letzten beobachteten Wert pro Tag-Kombination.

Die Netzwerk-Spielerzahl ist bereits global und wird nicht über alle Publisher addiert. GC-Werte ohne `gc`-Tag sind Collector-Gesamtwerte; eine leere GC-Spalte bezeichnet diese Gesamtserie. Sie wird nicht mit den Einzel-GCs addiert. Histogramm-Buckets werden separat nach `gc`/`le` angezeigt; diese überlappenden Buckets sind keine berechneten p95/p99-Werte.

RAM und JVM-Speicher sind Bytes, GC-/CPU-/Uptime-Dauern Sekunden und MSPT Millisekunden. `system:region:util` ist ein Anteil (Grafana `percentunit`); die übrigen CPU-/RAM-Prozentwerte sind bereits mit 100 skaliert. `system:regions:util` summiert Regionenauslastungen und kann über 100 % liegen. Der Prozessstart wird von Unix-Sekunden in Millisekunden für Grafanas Datumsformat umgerechnet.

## Zeitfenster und Last

Alle Graphen, Stat-Panels und Tabellen verwenden den oben im Dashboard ausgewählten Zeitraum. Es gibt keine individuellen Panel-Zeitfenster oder Zeitverschiebungen mehr. Der voreingestellte Dashboard-Zeitraum bleibt wie in deiner Vorlage auf 24 Stunden. Aktuelle Stat- und Tabellenwerte sind die letzten Beobachtungen innerhalb dieses gewählten Zeitraums; kumulative Spielersummen beginnen an dessen Anfang.

Der Collector schreibt etwa alle 10 Sekunden. Die Queries verwenden daher mindestens 10 Sekunden Buckets; das Dashboard aktualisiert sich alle 30 Sekunden. Viele Server/Regionen oder lange Zeitfenster können weiterhin die konfigurierten 50.000 Events pro Metrik oder 1.000 Serien überschreiten. Dann Filter auf Server/Welt/Region setzen oder den globalen Dashboard-Zeitraum verkleinern. Werte werden nicht still abgeschnitten. Die Zeitspalten der Tabellen sind über Feld-Overrides ausgeblendet und können zur Prüfung des Sample-Alters wieder eingeblendet werden.

## Reproduzieren und prüfen

```powershell
python build.py
python verify.py
node integration.mjs
```

Der Integrationstest verwendet die portable lokale Grafana-Testumgebung aus `grafana/.runtime`, startet eine isolierte FlameDB mit Test-Keys, importiert das v2-Dashboard und fragt jedes Panel mit synthetischen Collector-Samples ab. Er greift nicht auf deine Produktionsinstanz zu. Für eine laufende Vorschau vor dem Test `BETTERATTACK_PREVIEW=1` setzen; Ctrl+C beendet die Vorschauprozesse.
