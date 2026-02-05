package server

import _ "embed"

// Embedded static assets for the admin UI.
//
// We keep the admin UI offline-capable: no external CDN.

//go:embed static/chart.umd.min.js
var chartJS []byte

//go:embed static/logo.svg
var logoSVG []byte
