# WiCOS64 Remote Storage (Go)

Dieses Repository enthält einen kleinen Go-Backend-Server für **WiCOS64 Remote Storage**.
Dazu gehören:

- **wicos64-server**: HTTP-Backend (W64F / WiCOS64 API) inkl. optionalem Admin-UI
- **wicos64-tray**: Windows-Tray-Controller (Start/Stop/Restart + Admin UI öffnen)
- **w64tool**: kleines CLI-Testtool für die API

## Voraussetzungen

- Go **1.22+** (siehe `go.mod`)

## Quickstart (lokal)

1) Beispiel-Config kopieren und anpassen:

```bash
cp config/config.example.json config/config.json
# danach z.B. Tokens/Roots/Admin-Passwort anpassen
```

2) Server starten:

```bash
go run ./cmd/wicos64-server -config config/config.json -open-admin
```

- Default API-Endpoint (aus `config.example.json`): `http://127.0.0.1:8080/wicos64/api`
- Admin UI: `http://127.0.0.1:8080/admin`

> Hinweis: Wenn `config/config.json` fehlt, versucht der Server beim Start, sie aus
> `config/config.example.json` (oder Defaults) zu erzeugen.

## Build

Empfehlung: Binaries nach `./bin` bauen (ist in `.gitignore`).

```bash
mkdir -p bin

go build -o bin/wicos64-server ./cmd/wicos64-server

go build -o bin/w64tool ./cmd/w64tool

# Windows-Tray (unter Windows)
go build -o bin/wicos64-tray.exe ./cmd/wicos64-tray
```

## Konfiguration & Sicherheit

- `config/config.json` wird **nicht** eingecheckt (siehe `.gitignore`).
- Das Admin UI ist standardmäßig **nur lokal (localhost)** erreichbar (`admin_allow_remote=false`).
- Wenn du Remote-Zugriff aktivierst: **setz unbedingt ein `admin_password`** und stell das nicht ungeschützt ins Internet.

## Doku

PDFs liegen unter `./Docs` (DE/EN), z.B. Admin-Guide und API-Dokumentation.

## Lizenz
MIT License © 2026 TheRealWanderer

## Third‑party

Enthält **Chart.js** (MIT) als eingebettete statische Datei (`internal/server/static/chart.umd.min.js`, Lizenzhinweis steht im Header).
##


