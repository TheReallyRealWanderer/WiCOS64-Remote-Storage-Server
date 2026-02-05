package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"wicos64-server/internal/config"
	"wicos64-server/internal/version"
)

const adminPath = "/admin"

// --- Admin UI HTML (kept minimal, no build step) ---
//
// The UI deliberately does not use any external CDN assets.
// It also does not run any JS that would require permissions.
//
// NOTE: The admin UI is meant for *local* use. By default it is only accessible
// from localhost. See Config.AdminAllowRemote / AdminPassword.
const adminHTML = `<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WiCOS64 Remote Storage — Admin</title>
<style>
:root {
  --bg: #40318d;
  --fg: #a9b8ff;
  --fg2: #ffffff;
  --border: #a9b8ff;
  --danger: #ff6b6b;
  --warn: #ffd27d;
  --ok: #7CFC00;
  --panelbg: rgba(0,0,0,0.15);
}
html, body { height:100%; }
body {
  margin:0;
  background: var(--bg);
  color: var(--fg);
  font-family: ui-monospace, Menlo, Consolas, "Courier New", monospace;
  font-size: 14px;
  line-height: 1.25;
}
a { color: var(--fg2); }
header {
  padding: 12px 16px;
  border-bottom: 2px solid var(--border);
  background: rgba(0,0,0,0.12);
}
header .hdrrow {
  display:flex;
  justify-content: space-between;
  gap: 12px;
  align-items: flex-start;
}
header .hdrleft { min-width: 0; }
header .logoLink {
  display:inline-flex;
  align-items:flex-start;
  text-decoration:none;
}
header .logo {
  height: 44px;
  width: auto;
  opacity: 0.95;
}
header .logo:hover { opacity: 1; }
header .title {
  font-size: 16px;
  letter-spacing: 1px;
  color: var(--fg2);
}
header .sub {
  opacity:0.9;
  font-size: 12px;
  margin-top: 4px;
}
.container {
  max-width: 1300px;
  margin: 12px auto;
  padding: 0 12px;
}
.grid3 {
  display:grid;
  grid-template-columns: 1fr 1fr 1fr;
  gap: 12px;
}
.grid2 {
  display:grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
}
.cfgTabs button.active {
  border-color: var(--ok);
  color: var(--ok);
  box-shadow: 0 0 0 1px rgba(0,255,159,0.35) inset;
}
.cfgForm details {
  border: 1px dashed rgba(169,184,255,0.55);
  padding: 6px 8px;
  margin: 10px 0;
}
.cfgForm summary {
  cursor: pointer;
  color: var(--fg2);
  letter-spacing: 0.5px;
}
.cfgForm hr { border: none; border-top: 1px solid rgba(169,184,255,0.25); margin: 10px 0; }
.panel {
  border: 2px solid var(--border);
  background: var(--panelbg);
  padding: 10px;
}
.panel h2 {
  margin:0 0 8px;
  font-size: 13px;
  letter-spacing:1px;
  color: var(--fg2);
}
.small { font-size: 12px; opacity:0.9; }
.kv {
  display:grid;
  grid-template-columns: 120px 1fr;
  gap: 4px 8px;
}
.kv div { padding:1px 0; }
.warn { color: var(--warn); }
.bad { color: var(--danger); }
.good { color: var(--ok); }
hr { border:0; border-top: 1px solid rgba(169,184,255,0.3); margin: 8px 0; }
button {
  font-family: inherit;
  font-size: 13px;
  padding: 6px 10px;
  margin: 2px;
  border: 2px solid var(--border);
  background: rgba(0,0,0,0.2);
  color: var(--fg2);
  cursor:pointer;
}
button:hover { background: rgba(0,0,0,0.3); }
button.danger { border-color: var(--danger); color: var(--danger); }
button.ok { border-color: var(--ok); color: var(--ok); }
input, select, textarea {
  font-family: inherit;
  font-size: 13px;
  border: 2px solid var(--border);
  background: rgba(0,0,0,0.25);
  color: var(--fg2);
  padding: 4px 6px;
}
textarea { width: 100%; box-sizing:border-box; min-height: 240px; }
table { width: 100%; border-collapse: collapse; }
th, td {
  border: 1px solid rgba(169,184,255,0.4);
  padding: 4px 6px;
  vertical-align: top;
}
th {
  color: var(--fg2);
  background: rgba(0,0,0,0.18);
  text-align:left;
}
.badge {
  display:inline-block;
  padding: 1px 6px;
  border: 1px solid rgba(169,184,255,0.6);
}
.badge.warn { border-color: var(--warn); color: var(--warn); }
.badge.bad { border-color: var(--danger); color: var(--danger); }
.badge.good { border-color: var(--ok); color: var(--ok); }
.flex { display:flex; gap: 8px; flex-wrap: wrap; align-items:center; }
.logwrap { display:flex; gap: 10px; }
.loglist {
  width: 48%;
  min-width: 360px;
  max-width: 520px;
  border: 2px solid rgba(169,184,255,0.4);
  background: rgba(0,0,0,0.18);
  height: 420px;
  overflow:auto;
}
.logdetail {
  flex:1;
  border: 2px solid rgba(169,184,255,0.4);
  background: rgba(0,0,0,0.18);
  height: 420px;
  overflow:auto;
  padding: 8px;
}
.logline {
  padding: 4px 6px;
  border-bottom: 1px solid rgba(169,184,255,0.15);
  cursor:pointer;
  white-space: nowrap;
  overflow:hidden;
  text-overflow: ellipsis;
}
.logline:hover { background: rgba(0,0,0,0.25); }
.logline.active { background: rgba(0,0,0,0.35); outline: 1px solid var(--border); }
pre { margin:0; white-space: pre-wrap; word-break: break-word; }
.canvasBox { height: 200px; }
canvas { width: 100% !important; height: 200px !important; }

#toastWrap{
  position: fixed;
  top: 12px;
  right: 12px;
  z-index: 9999;
  display: flex;
  flex-direction: column;
  gap: 8px;
  pointer-events: none;
}
.toast{
  pointer-events: none;
  border: 2px solid var(--border);
  background: rgba(0,0,0,0.55);
  padding: 8px 10px;
  min-width: 240px;
  max-width: 460px;
  box-shadow: 0 2px 0 rgba(0,0,0,0.25);
  opacity: 0.98;
}
.toast.good { border-color: var(--ok); color: var(--ok); }
.toast.bad { border-color: var(--danger); color: var(--danger); }
.toast.warn { border-color: var(--warn); color: var(--warn); }

@media (max-width: 1100px) {
  .grid3 { grid-template-columns: 1fr; }
  .grid2 { grid-template-columns: 1fr; }
  .logwrap { flex-direction: column; }
  .loglist { width: 100%; max-width: none; min-width: 0; }
}
</style>
</head>
<body>
<div id="toastWrap"></div>
<header>
  <div class="hdrrow">
    <div class="hdrleft">
      <div class="title">WiCOS64 Remote Storage — Admin</div>
      <div class="sub" id="hdrSub">loading…</div>
    </div>
	    <a class="logoLink" href="/admin">
	      <img id="logoImg" class="logo" src="/admin/static/logo.svg" alt="WiCOS64" title="WiCOS64 Remote Storage Server">
	    </a>
  </div>
</header>
<div class="container">
  <div class="grid3">
    <div class="panel">
      <h2>STATUS</h2>
      <div class="kv">
        <div>Build</div><div id="build">-</div>
        <div>Listen</div><div id="listen">-</div>
        <div>Endpoint</div><div id="endpoint">-</div>
        <div>Uptime</div><div id="uptime">-</div>
        <div>Bootstrap</div><div id="bsState">-</div>
        <div>Trash</div><div id="trashState">-</div>
      </div>
      <div style="margin-top:8px" id="warnings"></div>
      <div style="margin-top:8px" class="small" id="statusMsg"></div>
      <div class="hr"></div>
      <h3>BOOTSTRAP</h3>
      <div class="small">
        <div>Example (LAN):</div>
        <div class="mono" id="bsURL">-</div>
      </div>
      <div class="kv">
        <div>CFG token</div><div id="bsToken">-</div>
        <div>MAC maps</div><div id="bsMaps">-</div>
      </div>
    </div>
    <div class="panel">
      <h2>CONTROLS</h2>
      <div class="flex">
        <button class="ok" onclick="actionReload()">SOFT RESTART (Reload Config)</button>
        <button onclick="actionCleanup()">Run .TMP Cleanup</button>
        <button onclick="actionSelfTest()">Self-Test</button>
        <button onclick="actionStatsReset()">Reset Stats</button>
        <button class="danger" onclick="actionLogsClear()">Clear Logs</button>
        <button onclick="actionLogsExport()">Export Logs</button>
      </div>
      <div style="margin-top:10px" class="small">
        Hint: Admin UI is offline (no CDN). Charts use embedded Chart.js.
      </div>
      <div style="margin-top:10px" id="actionOut"><pre></pre></div>
    </div>
    <div class="panel">
      <h2>STATS</h2>
      <div class="kv">
        <div>Requests</div><div id="statReq">-</div>
        <div>Errors</div><div id="statErr">-</div>
        <div>Avg ms</div><div id="statAvg">-</div>
        <div>Bytes In</div><div id="statIn">-</div>
        <div>Bytes Out</div><div id="statOut">-</div>
      </div>
      <div style="margin-top:8px" class="small">Charts below update every ~2 seconds.</div>
    </div>

    
  </div>

  <div style="height:12px"></div>

  <div class="grid2">
    <div class="panel">
      <h2>CHARTS</h2>
      <div class="canvasBox"><canvas id="chartReq"></canvas></div>
      <div style="height:10px"></div>
      <div class="canvasBox"><canvas id="chartBytes"></canvas></div>
    </div>
    <div class="panel">
      <h2>TOKENS & QUOTAS</h2>
      <div class="small" id="tokenMeta"></div>
      <div style="overflow:auto; max-height: 420px;">
        <table id="tokenTable">
          <thead>
            <tr>
              <th>Kind</th>
              <th>Name</th>
              <th>Token</th>
              <th>Root</th>
              <th>Flags</th>
              <th>Quota</th>
              <th>Used</th>
              <th>.TMP</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="hr"></div>
      <h3>TOKEN MANAGER</h3>
      <div class="small">Edit tokens[] without manually editing JSON.</div>
      <div class="grid2" style="grid-template-columns: 1.2fr 1.8fr; gap:10px;">
        <div>
          <select id="tokSel" size="7" style="width:100%;"></select>
          <div class="flex" style="margin-top:6px">
            <button class="btn" onclick="tokNew()">New</button>
            <button class="btn" onclick="tokRefresh()">Refresh</button>
            <button class="btn" onclick="tokDelete()">Delete</button>
          </div>
        </div>
        <div>
          <div class="flex">
            <label class="small">Name<br><input id="tokName" placeholder="e.g. C64 #1"></label>
            <label class="small">Token<br><input id="tokToken" placeholder="secret"></label>
            <button class="btn" style="align-self:flex-end" onclick="tokGen()">Generate</button>
          </div>
          <div class="flex">
            <label class="small">Root<br><input id="tokRoot" placeholder="/"></label>
            <label class="small">Quota (bytes, 0=off)<br><input id="tokQuota" placeholder="0"></label>
            <label class="small">Max file (bytes, 0=off)<br><input id="tokMaxFile" placeholder="0"></label>
          </div>
          <div class="flex">
            <label class="small"><input type="checkbox" id="tokEnabled" checked> Enabled</label>
            <label class="small"><input type="checkbox" id="tokReadOnly"> Read-only</label>
            <label class="small">Disk images (.D64/.D71/.D81)<br><select id="tokDiskImages"><option value="">(inherit)</option><option value="true">true</option><option value="false">false</option></select></label>
            <label class="small">Disk images write (.D64/.D71/.D81)<br><select id="tokDiskImagesWrite"><option value="">(inherit)</option><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">Disk images auto-resize (D81 subdirs)<br><select id="tokDiskImagesAutoResize"><option value="">(inherit)</option><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small"><input type="checkbox" id="tokDiskImagesRenameConvert"> Allow rename convert (.D64/.D71/.D81)</label>
          </div>
          <div class="flex">
            <button class="btn" onclick="tokSave()">Save</button>
            <span class="small">Writes config.json and applies immediately (listen/endpoint need restart).</span>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div style="height:12px"></div>

  <div class="panel">
    <h2>LIVE LOG</h2>
    <div class="flex small">
      <label>Op
        <select id="fOp">
          <option value="">(any)</option>
          <!-- Values are W64F opcode bytes in HEX (v0.2.x+) -->
          <option value="0D">PING</option>
          <option value="0E">CAPS</option>
          <option value="0F">STATFS</option>
          <option value="01">LS</option>
          <option value="02">STAT</option>
          <option value="03">READ</option>
          <option value="04">WRITE</option>
          <option value="05">APPEND</option>
          <option value="06">MKDIR</option>
          <option value="07">RMDIR</option>
          <option value="08">RM</option>
          <option value="09">CP</option>
          <option value="0A">MV</option>
          <option value="0B">SEARCH</option>
          <option value="0C">HASH</option>
        </select>
      </label>
      <label>Errors <input type="checkbox" id="fErr" /></label>
      <label>IP <input id="fIP" placeholder="contains…" size="14"></label>
      <label>Search <input id="fQ" placeholder="in info / preview…" size="22"></label>
      <label>Limit <input id="fLimit" value="300" size="6"></label>
      <button onclick="reloadLogs()">Reload</button>
      <span id="logMeta"></span>
    </div>
    <div style="height:8px"></div>
    <div class="logwrap">
      <div class="loglist" id="logList"></div>
      <div class="logdetail">
        <div class="flex small" style="justify-content:space-between; align-items:center; gap:8px; margin-bottom:6px">
          <span id="logSelMeta" class="muted">(no selection)</span>
          <button id="logToOpsBtn" onclick="logToOps()" title="Send selected request to Ops Playground" disabled>To Playground</button>
        </div>
        <pre id="logDetail">(click a log line)</pre>
      </div>
    </div>

    <details id="opsDetails">
      <summary>OPS PLAYGROUND</summary>
      <div class="small">
        <div class="flex small" style="align-items:flex-end">
          <label>Token
            <select id="opsToken" style="min-width:220px"></select>
          </label>
          <label>Command
            <input id="opsCmd" placeholder="ls /" size="42" autocomplete="off">
          </label>
          <button id="opsRunBtn" class="ok" onclick="opsRun()">Run</button>
          <button id="opsClearBtn" onclick="opsClear()">Clear</button>
          <span id="opsMeta" class="small muted"></span>
        </div>
        <div style="height:6px"></div>
        <div class="grid2">
          <div>
            <div class="small muted">Data (optional for <code>write</code>/<code>append</code>). Encoding:
              <select id="opsDataEnc">
                <option value="text">text</option>
                <option value="hex">hex</option>
                <option value="base64">base64</option>
              </select>
            </div>
            <textarea id="opsData" rows="3" style="width:100%; resize:vertical" placeholder="(optional)"></textarea>
            <div class="small muted" style="margin-top:6px">
              Examples:
              <code>caps</code>,
              <code>ping</code>,
              <code>ls /</code>,
              <code>ls /dev1 0 200</code>,
              <code>stat /dev1/datei.prg</code>,
              <code>read /dev1/datei.prg 0 64</code>,
              <code>cp -o /disk.d81/DATEI.PRG /dev1/</code>,
              <code>cp -o /dev1/* /disk.d81/</code>
            </div>
          </div>
          <div>
            <pre id="opsOut" style="height:160px; overflow:auto; white-space:pre-wrap; margin:0; padding:8px; border:1px solid #334; background:#0b0b1a"></pre>
          </div>
        </div>
      </div>
    </details>

  </div>

  <div style="height:12px"></div>

  <div class="panel">
    <h2>CONFIG</h2>
    <div class="flex small">
      <button onclick="loadConfig()">Reload from server</button>
      <button class="ok" onclick="saveConfigUI()">Save config.json</button>
      <span id="cfgMeta"></span>
    </div>

    <div class="flex small cfgTabs">
      <span class="badge">View</span>
      <button id="cfgTabFormBtn" onclick="cfgShowTab('form')">Form</button>
      <button id="cfgTabJsonBtn" onclick="cfgShowTab('json')">JSON</button>
      <button onclick="cfgSyncFormFromText()" title="Rebuild the form from JSON">JSON → Form</button>
      <button onclick="cfgSyncTextFromForm()" title="Update JSON from the form">Form → JSON</button>
      <span class="small">Form is the recommended editor. JSON is for advanced tweaks.</span>
    </div>

    <div id="cfgForm" class="cfgForm">
		<details>
			<summary>SERVER</summary>
			<div class="grid2">
				<label class="small">Listen<br><input id="cfgListen" placeholder=":8080"></label>
				<label class="small">Endpoint<br><input id="cfgEndpoint" placeholder="/wicos64/api"></label>
				<label class="small">Base path (storage root)<br><input id="cfgBasePath" placeholder="./data"></label>
				<label class="small">Server name<br><input id="cfgServerName" placeholder="WiCOS64 Remote Storage"></label>
				<label class="small">Legacy token (optional)<br><input id="cfgLegacyToken" placeholder="CHANGE-ME"></label>
			</div>
		</details>

		<details>
			<summary>LIMITS & POLICIES</summary>
			<div class="grid3">
				<label class="small">Max payload (bytes)<br><input id="cfgMaxPayload" type="number" min="0"></label>
				<label class="small">Max chunk (bytes)<br><input id="cfgMaxChunk" type="number" min="0"></label>
				<label class="small">Max entries (LS/SEARCH)<br><input id="cfgMaxEntries" type="number" min="0"></label>
				<label class="small">Max path length<br><input id="cfgMaxPath" type="number" min="0"></label>
				<label class="small">Max name length<br><input id="cfgMaxName" type="number" min="0"></label>
				<label class="small">Global max file (bytes, 0=off)<br><input id="cfgGlobalMaxFile" type="number" min="0"></label>
				<label class="small">Global quota (bytes, 0=off)<br><input id="cfgGlobalQuota" type="number" min="0"></label>
				<label class="small">Global read-only<br>
					<select id="cfgGlobalReadOnly">
						<option value="false">false</option>
						<option value="true">true</option>
					</select>
				</label>
			</div>
		</details>

		<details>
			<summary>FEATURE TOGGLES</summary>
			<div class="grid3">
				<label class="small">mkdir parents<br><select id="cfgMkdirParents"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">rmdir recursive<br><select id="cfgRmdirRecursive"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">cp recursive<br><select id="cfgCpRecursive"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">overwrite allowed<br><select id="cfgOverwrite"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">error messages in response<br><select id="cfgErrMsg"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">create recommended dirs<br><select id="cfgRecDirs"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">log requests<br><select id="cfgLogRequests"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">disk images (.D64/.D71/.D81)<br><select id="cfgDiskImages"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">disk images writable (.D64/.D71/.D81)<br><select id="cfgDiskImagesWrite"><option value="false">false</option><option value="true">true</option></select></label>
				<label class="small">disk images auto-resize (D81 subdirs)<br><select id="cfgDiskImagesAutoResize"><option value="false">false</option><option value="true">true</option></select></label>
			</div>
		</details>

		<details>
			<summary>ADMIN UI</summary>
			<div class="grid3">
				<label class="small">enable admin UI<br><select id="cfgEnableAdmin"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">allow remote admin<br><select id="cfgAdminRemote"><option value="false">false</option><option value="true">true</option></select></label>
				<label class="small">admin user<br><input id="cfgAdminUser" placeholder="admin"></label>
				<label class="small">admin password<br><input id="cfgAdminPass" type="password" placeholder="(empty=off)"></label>
			</div>
			<div class="small" style="margin-top:6px; opacity:0.85;">
				Tip: If you enable remote admin, set a strong password.
			</div>
		</details>

		<details>
			<summary>BOOTSTRAP (LAN-only config fetch)</summary>
			<div class="grid3">
				<label class="small">enabled<br><select id="cfgBsEnabled"><option value="false">false</option><option value="true">true</option></select></label>
				<label class="small">allow GET<br><select id="cfgBsAllowGet"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">allow POST<br><select id="cfgBsAllowPost"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">LAN only<br><select id="cfgBsLanOnly"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">unknown MAC policy<br>
					<select id="cfgBsPolicy">
						<option value="deny">deny</option>
						<option value="legacy">legacy</option>
					</select>
				</label>
				<label class="small">config token<br><input id="cfgBsToken" placeholder="CFG-1234"></label>
			</div>
			<div style="height:6px"></div>
			<div class="flex small">
				<span class="badge">MAC→Token map</span>
				<button onclick="cfgBsAddRow('', '')">Add</button>
				<span class="small">MAC can be entered as AABBCCDDEEFF or with colons/dashes (it will be normalized on save).</span>
			</div>
			<table class="small">
				<thead><tr><th style="width:220px">MAC</th><th>Token</th><th style="width:70px">Del</th></tr></thead>
				<tbody id="cfgBsMacBody"></tbody>
			</table>
			<div style="height:10px"></div>
			<div class="flex small">
				<span class="badge">MAC→Extra (optional KEY=VALUE lines)</span>
				<button onclick="cfgBsExtraAddRow('', '')">Add</button>
				<span class="small">These lines are appended before <span class="badge">END</span> in the bootstrap response for that MAC.</span>
			</div>
			<table class="small">
				<thead><tr><th style="width:220px">MAC</th><th>Extra</th><th style="width:70px">Del</th></tr></thead>
				<tbody id="cfgBsExtraBody"></tbody>
			</table>
		</details>

		<details>
			<summary>DISCOVERY (UDP, WDP1)</summary>
			<div class="grid3">
				<label class="small">enabled<br><select id="cfgDiscEnabled"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">UDP port<br><input id="cfgDiscPort" type="number" min="1" max="65535"></label>
				<label class="small">LAN only<br><select id="cfgDiscLanOnly"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">rate limit (/sec, 0=off)<br><input id="cfgDiscRate" type="number" min="0"></label>
			</div>
			<div class="small" style="margin-top:6px; opacity:0.85;">
				Listens on UDP port 6464 and answers WDP1 discovery packets so a C64 can find the HTTP bootstrap endpoint automatically.
			</div>
		</details>

		<details>
			<summary>MAINTENANCE</summary>
			<div class="grid3">
				<label class="small">tmp cleanup enabled<br><select id="cfgTmpCleanup"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">tmp interval (sec)<br><input id="cfgTmpInt" type="number" min="0"></label>
				<label class="small">tmp max age (sec)<br><input id="cfgTmpAge" type="number" min="0"></label>
				<label class="small">tmp delete empty dirs<br><select id="cfgTmpEmpty"><option value="true">true</option><option value="false">false</option></select></label>
			</div>
			<hr>
			<div class="grid3">
				<label class="small">trash enabled<br><select id="cfgTrash"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">trash dir<br><input id="cfgTrashDir" placeholder="./trash"></label>
				<label class="small">trash cleanup enabled<br><select id="cfgTrashCleanup"><option value="true">true</option><option value="false">false</option></select></label>
				<label class="small">trash interval (sec)<br><input id="cfgTrashInt" type="number" min="0"></label>
				<label class="small">trash max age (sec)<br><input id="cfgTrashAge" type="number" min="0"></label>
				<label class="small">trash delete empty dirs<br><select id="cfgTrashEmpty"><option value="true">true</option><option value="false">false</option></select></label>
			</div>
		</details>

		<details>
			<summary>COMPATIBILITY</summary>
			<div class="grid2">
				<label class="small">Fallback to .PRG when missing<br>
					<select id="cfgCompatPrg"><option value="true">true</option><option value="false">false</option></select>
				</label>
				<div class="small" style="align-self:end; opacity:0.85;">
					If a client requests <span class="badge">/DATEI</span> and it doesn't exist, the server will try <span class="badge">/DATEI.PRG</span>.
				</div>
			</div>
			<hr>
			<div class="grid2">
				<label class="small">Wildcard LOAD (*, ?)<br>
					<select id="cfgCompatWildcard"><option value="true">true</option><option value="false">false</option></select>
				</label>
				<div class="small" style="align-self:end; opacity:0.85;">
					If a client requests <span class="badge">/DATEI*</span> or <span class="badge">/DATEI?</span>, the server resolves it to the first matching file in that directory.
				</div>
			</div>
		</details>
    </div>

    <div id="cfgJson" style="display:none">
      <textarea id="cfgText" spellcheck="false"></textarea>
    </div>
  </div>

</div>

<script src="/admin/static/chart.umd.min.js"></script>
<script>
'use strict';

function el(id){ return document.getElementById(id); }

function fmtBytes(n){
  if (n === null || n === undefined) return '-';
  var u = ['B','KiB','MiB','GiB','TiB'];
  var x = Number(n);
  var i = 0;
  while (x >= 1024 && i < u.length-1){ x = x/1024; i++; }
  var s = (i===0) ? String(Math.round(x)) : x.toFixed(1);
  return s + ' ' + u[i];
}

function maskTok(t){
  if (!t) return '';
  t = String(t);
  if (t.length <= 8) return t;
  return t.slice(0,4) + '…' + t.slice(-4);
}

// ------------------------- CONFIG (form + JSON) -------------------------

var cfgTab = 'form';
var cfgFormSyncing = false;
var cfgFormSyncTimer = null;

function boolToSel(v){ return v ? 'true' : 'false'; }
function selToBool(v){ return String(v) === 'true'; }

function normMac(s){
  if (!s) return '';
  return String(s).toUpperCase().replace(/[^0-9A-F]/g, '');
}

function cfgGetObjOrNull(){
  try {
    return JSON.parse(el('cfgText').value || '{}');
  } catch(e){
    setStatus('config JSON invalid: ' + e.message, 'bad');
    return null;
  }
}

function cfgWriteObj(obj){
  el('cfgText').value = JSON.stringify(obj, null, 2);
}

function cfgShowTab(tab){
  if (tab === cfgTab) return;
  // Keep both views in sync on tab switches.
  if (tab === 'json') cfgSyncTextFromForm(true);
  if (tab === 'form') cfgSyncFormFromText(true);

  cfgTab = tab;
  el('cfgForm').style.display = (tab === 'form') ? 'block' : 'none';
  el('cfgJson').style.display = (tab === 'json') ? 'block' : 'none';

  el('cfgTabFormBtn').classList.toggle('active', tab === 'form');
  el('cfgTabJsonBtn').classList.toggle('active', tab === 'json');
}

function cfgInitForm(){
  // Default view (do not sync yet; loadConfig() will fill the form).
  cfgTab = 'form';
  el('cfgForm').style.display = 'block';
  el('cfgJson').style.display = 'none';
  el('cfgTabFormBtn').classList.add('active');
  el('cfgTabJsonBtn').classList.remove('active');

  // Bind form change events (debounced) so token editing and other UI parts
  // always operate on the latest config JSON.
  var wrap = el('cfgForm');
  if (!wrap) return;
  wrap.addEventListener('input', cfgFormChanged, true);
  wrap.addEventListener('change', cfgFormChanged, true);
}

function cfgFormChanged(){
  if (cfgFormSyncing) return;
  if (cfgFormSyncTimer) clearTimeout(cfgFormSyncTimer);
  cfgFormSyncTimer = setTimeout(function(){
    cfgSyncTextFromForm(false);
  }, 300);
}

function cfgSetVal(id, v){
  var x = el(id);
  if (!x) return;
  x.value = (v === undefined || v === null) ? '' : String(v);
}

function cfgSetBoolSel(id, v){
  var x = el(id);
  if (!x) return;
  x.value = boolToSel(!!v);
}

function cfgGetStr(id){ return (el(id).value || '').trim(); }
function cfgGetNum(id){
  var s = (el(id).value || '').trim();
  if (s === '') return 0;
  var n = parseInt(s, 10);
  return isNaN(n) ? 0 : n;
}
function cfgGetBoolSel(id){ return selToBool(el(id).value); }

function cfgBsClearRows(){
  el('cfgBsMacBody').innerHTML = '';
}

function cfgBsExtraClearRows(){
  el('cfgBsExtraBody').innerHTML = '';
}

function cfgBsAddRow(mac, tok){
  var body = el('cfgBsMacBody');
  var tr = document.createElement('tr');

  var td1 = document.createElement('td');
  var i1 = document.createElement('input');
  i1.value = mac || '';
  i1.size = 14;
  td1.appendChild(i1);

  var td2 = document.createElement('td');
  var i2 = document.createElement('input');
  i2.value = tok || '';
  i2.size = 22;
  td2.appendChild(i2);

  var td3 = document.createElement('td');
  var b = document.createElement('button');
  b.textContent = 'X';
  b.className = 'danger';
  b.onclick = function(){ tr.remove(); cfgFormChanged(); };
  td3.appendChild(b);

  tr.appendChild(td1);
  tr.appendChild(td2);
  tr.appendChild(td3);
  body.appendChild(tr);
  cfgFormChanged();
}

function cfgBsGetMap(){
  var body = el('cfgBsMacBody');
  var rows = body.querySelectorAll('tr');
  var m = {};
  for (var i=0; i<rows.length; i++){
    var ins = rows[i].querySelectorAll('input');
    if (ins.length < 2) continue;
    var mac = normMac(ins[0].value);
    var tok = String(ins[1].value || '').trim();
    if (!mac && !tok) continue;
    if (!mac) continue;
    if (!tok) continue;
    m[mac] = tok;
  }
  return m;
}

function cfgBsExtraAddRow(mac, text){
  var body = el('cfgBsExtraBody');
  var tr = document.createElement('tr');

  var td1 = document.createElement('td');
  var i1 = document.createElement('input');
  i1.value = mac || '';
  i1.size = 14;
  td1.appendChild(i1);

  var td2 = document.createElement('td');
  var t = document.createElement('textarea');
  t.value = text || '';
  t.spellcheck = false;
  t.style.minHeight = '72px';
  t.style.height = '72px';
  td2.appendChild(t);

  var td3 = document.createElement('td');
  var b = document.createElement('button');
  b.textContent = 'X';
  b.className = 'danger';
  b.onclick = function(){ tr.remove(); cfgFormChanged(); };
  td3.appendChild(b);

  tr.appendChild(td1);
  tr.appendChild(td2);
  tr.appendChild(td3);
  body.appendChild(tr);
  cfgFormChanged();
}

function cfgBsExtraGetMap(){
  var body = el('cfgBsExtraBody');
  var rows = body.querySelectorAll('tr');
  var out = {};
  for (var i=0; i<rows.length; i++){
    var ins = rows[i].querySelectorAll('input');
    var tas = rows[i].querySelectorAll('textarea');
    if (ins.length < 1 || tas.length < 1) continue;
    var mac = normMac(ins[0].value);
    var txt = String(tas[0].value || '');
    if (!mac && !txt.trim()) continue;
    if (!mac) continue;

    var m = {};
    var lines = txt.split(/\r?\n/);
    for (var j=0; j<lines.length; j++){
      var line = String(lines[j] || '').trim();
      if (!line) continue;
      if (line[0] === '#') continue;
      var k, v;
      var p = line.indexOf('=');
      if (p < 0) continue;
      k = line.slice(0, p).trim();
      v = line.slice(p+1).trim();
      if (!k) continue;
      m[k] = v;
    }
    if (Object.keys(m).length > 0) out[mac] = m;
  }
  return out;
}

function cfgApplyObjToForm(obj){
  cfgFormSyncing = true;
  try {
    obj = obj || {};

    cfgSetVal('cfgListen', obj.listen);
    cfgSetVal('cfgEndpoint', obj.endpoint);
    cfgSetVal('cfgBasePath', obj.base_path);
    cfgSetVal('cfgServerName', obj.server_name);
    cfgSetVal('cfgLegacyToken', obj.token);

    cfgSetVal('cfgMaxPayload', obj.max_payload);
    cfgSetVal('cfgMaxChunk', obj.max_chunk);
    cfgSetVal('cfgMaxEntries', obj.max_entries);
    cfgSetVal('cfgMaxPath', obj.max_path);
    cfgSetVal('cfgMaxName', obj.max_name);
    cfgSetVal('cfgGlobalMaxFile', obj.global_max_file_bytes);
    cfgSetVal('cfgGlobalQuota', obj.global_quota_bytes);
    cfgSetBoolSel('cfgGlobalReadOnly', obj.global_read_only);

    cfgSetBoolSel('cfgMkdirParents', obj.enable_mkdir_parents);
    cfgSetBoolSel('cfgRmdirRecursive', obj.enable_rmdir_recursive);
    cfgSetBoolSel('cfgCpRecursive', obj.enable_cp_recursive);
    cfgSetBoolSel('cfgOverwrite', obj.enable_overwrite);
    cfgSetBoolSel('cfgErrMsg', obj.enable_errmsg);
    cfgSetBoolSel('cfgRecDirs', obj.create_recommended_dirs);
    cfgSetBoolSel('cfgLogRequests', obj.log_requests);
				cfgSetBoolSel('cfgDiskImages', obj.disk_images_enabled !== false);
				cfgSetBoolSel('cfgDiskImagesWrite', obj.disk_images_write_enabled === true);
				cfgSetBoolSel('cfgDiskImagesAutoResize', obj.disk_images_auto_resize_enabled === true);

    cfgSetBoolSel('cfgEnableAdmin', obj.enable_admin_ui);
    cfgSetBoolSel('cfgAdminRemote', obj.admin_allow_remote);
    cfgSetVal('cfgAdminUser', obj.admin_user);
    cfgSetVal('cfgAdminPass', obj.admin_password);

    var bs = obj.bootstrap || {};
    cfgSetBoolSel('cfgBsEnabled', bs.enabled);
	    cfgSetBoolSel('cfgBsAllowGet', bs.allow_get);
	    cfgSetBoolSel('cfgBsAllowPost', bs.allow_post);
    cfgSetBoolSel('cfgBsLanOnly', bs.lan_only);
    cfgSetVal('cfgBsToken', bs.token);
    cfgSetVal('cfgBsPolicy', bs.unknown_mac_policy || 'deny');
    cfgBsClearRows();
    cfgBsExtraClearRows();
    if (bs.mac_tokens){
      var ks = Object.keys(bs.mac_tokens);
      ks.sort();
      for (var i=0; i<ks.length; i++){
        var k = ks[i];
        cfgBsAddRow(k, bs.mac_tokens[k]);
      }
    }
    if (bs.mac_extra){
      var ms = Object.keys(bs.mac_extra);
      ms.sort();
      for (var i=0; i<ms.length; i++){
        var mac = ms[i];
        var kv = bs.mac_extra[mac] || {};
        var ks2 = Object.keys(kv);
        ks2.sort();
        var lines = [];
        for (var j=0; j<ks2.length; j++){
          var kk = ks2[j];
          lines.push(kk + '=' + String(kv[kk]));
        }
        cfgBsExtraAddRow(mac, lines.join('\n'));
      }
    }

    var disc = obj.discovery || {};
    cfgSetBoolSel('cfgDiscEnabled', disc.enabled);
    cfgSetVal('cfgDiscPort', disc.udp_port);
    cfgSetBoolSel('cfgDiscLanOnly', disc.lan_only);
    cfgSetVal('cfgDiscRate', disc.rate_limit_per_sec);

    cfgSetBoolSel('cfgTmpCleanup', obj.tmp_cleanup_enabled);
    cfgSetVal('cfgTmpInt', obj.tmp_cleanup_interval_sec);
    cfgSetVal('cfgTmpAge', obj.tmp_cleanup_max_age_sec);
    cfgSetBoolSel('cfgTmpEmpty', obj.tmp_cleanup_delete_empty_dirs);

    cfgSetBoolSel('cfgTrash', obj.trash_enabled);
    cfgSetVal('cfgTrashDir', obj.trash_dir);
    cfgSetBoolSel('cfgTrashCleanup', obj.trash_cleanup_enabled);
    cfgSetVal('cfgTrashInt', obj.trash_cleanup_interval_sec);
    cfgSetVal('cfgTrashAge', obj.trash_cleanup_max_age_sec);
    cfgSetBoolSel('cfgTrashEmpty', obj.trash_cleanup_delete_empty_dirs);

    var compat = obj.compat || {};
    cfgSetBoolSel('cfgCompatPrg', compat.fallback_prg_extension);
  cfgSetBoolSel('cfgCompatWildcard', (compat.wildcard_load !== false));

  } finally {
    cfgFormSyncing = false;
  }
}

function cfgApplyFormToObj(obj){
  obj = obj || {};

  obj.listen = cfgGetStr('cfgListen');
  obj.endpoint = cfgGetStr('cfgEndpoint');
  obj.base_path = cfgGetStr('cfgBasePath');
  obj.server_name = cfgGetStr('cfgServerName');
  obj.token = cfgGetStr('cfgLegacyToken');

  obj.max_payload = cfgGetNum('cfgMaxPayload');
  obj.max_chunk = cfgGetNum('cfgMaxChunk');
  obj.max_entries = cfgGetNum('cfgMaxEntries');
  obj.max_path = cfgGetNum('cfgMaxPath');
  obj.max_name = cfgGetNum('cfgMaxName');
  obj.global_max_file_bytes = cfgGetNum('cfgGlobalMaxFile');
  obj.global_quota_bytes = cfgGetNum('cfgGlobalQuota');
  obj.global_read_only = cfgGetBoolSel('cfgGlobalReadOnly');

  obj.enable_mkdir_parents = cfgGetBoolSel('cfgMkdirParents');
  obj.enable_rmdir_recursive = cfgGetBoolSel('cfgRmdirRecursive');
  obj.enable_cp_recursive = cfgGetBoolSel('cfgCpRecursive');
  obj.enable_overwrite = cfgGetBoolSel('cfgOverwrite');
  obj.enable_errmsg = cfgGetBoolSel('cfgErrMsg');
  obj.create_recommended_dirs = cfgGetBoolSel('cfgRecDirs');
  obj.log_requests = cfgGetBoolSel('cfgLogRequests');
  obj.disk_images_enabled = cfgGetBoolSel('cfgDiskImages');
  obj.disk_images_write_enabled = cfgGetBoolSel('cfgDiskImagesWrite');
  obj.disk_images_auto_resize_enabled = cfgGetBoolSel('cfgDiskImagesAutoResize');

  obj.enable_admin_ui = cfgGetBoolSel('cfgEnableAdmin');
  obj.admin_allow_remote = cfgGetBoolSel('cfgAdminRemote');
  obj.admin_user = cfgGetStr('cfgAdminUser');
  obj.admin_password = cfgGetStr('cfgAdminPass');

  obj.bootstrap = obj.bootstrap || {};
  obj.bootstrap.enabled = cfgGetBoolSel('cfgBsEnabled');
  obj.bootstrap.allow_get = cfgGetBoolSel('cfgBsAllowGet');
  obj.bootstrap.allow_post = cfgGetBoolSel('cfgBsAllowPost');
  obj.bootstrap.lan_only = cfgGetBoolSel('cfgBsLanOnly');
  obj.bootstrap.token = cfgGetStr('cfgBsToken');
  obj.bootstrap.unknown_mac_policy = (el('cfgBsPolicy').value || 'deny');
  obj.bootstrap.mac_tokens = cfgBsGetMap();
  obj.bootstrap.mac_extra = cfgBsExtraGetMap();

  obj.discovery = obj.discovery || {};
  obj.discovery.enabled = cfgGetBoolSel('cfgDiscEnabled');
  obj.discovery.udp_port = cfgGetNum('cfgDiscPort');
  obj.discovery.lan_only = cfgGetBoolSel('cfgDiscLanOnly');
  obj.discovery.rate_limit_per_sec = cfgGetNum('cfgDiscRate');

  obj.tmp_cleanup_enabled = cfgGetBoolSel('cfgTmpCleanup');
  obj.tmp_cleanup_interval_sec = cfgGetNum('cfgTmpInt');
  obj.tmp_cleanup_max_age_sec = cfgGetNum('cfgTmpAge');
  obj.tmp_cleanup_delete_empty_dirs = cfgGetBoolSel('cfgTmpEmpty');

  obj.trash_enabled = cfgGetBoolSel('cfgTrash');
  obj.trash_dir = cfgGetStr('cfgTrashDir');
  obj.trash_cleanup_enabled = cfgGetBoolSel('cfgTrashCleanup');
  obj.trash_cleanup_interval_sec = cfgGetNum('cfgTrashInt');
  obj.trash_cleanup_max_age_sec = cfgGetNum('cfgTrashAge');
  obj.trash_cleanup_delete_empty_dirs = cfgGetBoolSel('cfgTrashEmpty');

  obj.compat = obj.compat || {};
  obj.compat.fallback_prg_extension = cfgGetBoolSel('cfgCompatPrg');
  obj.compat.wildcard_load = cfgGetBoolSel('cfgCompatWildcard');

  return obj;
}

function cfgSyncFormFromText(verbose){
  var obj = cfgGetObjOrNull();
  if (!obj) return;
  cfgApplyObjToForm(obj);
  if (verbose) setStatus('form synced from JSON', 'good');
}

function cfgSyncTextFromForm(verbose){
  var obj = cfgGetObjOrNull();
  if (!obj) obj = {};
  cfgApplyFormToObj(obj);
  cfgWriteObj(obj);
  // Keep token manager list in sync with the textarea.
  tokRefresh();
  if (verbose) setStatus('JSON synced from form', 'good');
}

function saveConfigUI(){
  if (cfgTab === 'form') cfgSyncTextFromForm(false);
  return saveConfig();
}

function renderBootstrap(cfg){
  cfg = cfg || {};
  var bs = cfg.bootstrap || {};
  var token = bs.token || '';
  var mapCount = bs.mac_tokens ? Object.keys(bs.mac_tokens).length : 0;

  if (bs.enabled){
    var extra = bs.lan_only ? ' LAN_ONLY' : '';
    var meth = [];
    if (bs.allow_get !== false) meth.push('GET');
    if (bs.allow_post !== false) meth.push('POST');
    var mtxt = meth.length ? (' ' + meth.join('+')) : ' (no methods)';
    el('bsState').innerHTML = '<span class="badge good">ENABLED</span>' + extra + ' <span class="badge">' + mtxt.trim() + '</span>';

    var getURL = window.location.origin + '/wicos64/bootstrap?cfg=' + encodeURIComponent(token) + '&mac=AABBCCDDEEFF';
    var postURL = window.location.origin + '/wicos64/bootstrap';
    var lines = [];
    if (bs.allow_get !== false) lines.push('GET  ' + getURL);
    if (bs.allow_post !== false) lines.push('POST ' + postURL + '  (body: cfg=' + encodeURIComponent(token) + '&mac=AABBCCDDEEFF)');
    el('bsURL').innerHTML = lines.join('<br>');
  } else {
    el('bsState').innerHTML = '<span class="badge warn">OFF</span>';
    el('bsURL').textContent = '-';
  }

  el('bsToken').textContent = token ? maskTok(token) : '-';
  el('bsMaps').textContent = String(mapCount);
}

function renderTrash(cfg){
  cfg = cfg || {};
  if (!cfg.trash_enabled){
    el('trashState').innerHTML = '<span class="badge warn">OFF</span>';
    return;
  }
  var td = (cfg.trash_dir || '.TRASH').trim();
  el('trashState').innerHTML = '<span class="badge good">ON</span> ' + td;
}

var tokEditIndex = -1;

function tokReadConfig(){
  try {
    return JSON.parse(el('cfgText').value);
  } catch (e){
    return null;
  }
}

function tokWriteConfig(cfg){
  el('cfgText').value = JSON.stringify(cfg, null, 2);
  cfgSyncFormFromText(false);
}

function tokRefresh(){
  var cfg = tokReadConfig();
  var sel = el('tokSel');
  sel.innerHTML = '';
  tokEditIndex = -1;
  if (!cfg){
    return;
  }
  var list = cfg.tokens || [];
  for (var i=0;i<list.length;i++){
    var t = list[i] || {};
    var name = (t.name || '').trim() || '(unnamed)';
    var root = (t.root || '/').trim() || '/';
    var opt = document.createElement('option');
    opt.value = String(i);
    opt.textContent = name + ' | ' + maskTok(t.token) + ' | ' + root;
    sel.appendChild(opt);
  }
  sel.onchange = tokSelect;
}

function tokClearForm(){
  el('tokName').value = '';
  el('tokToken').value = '';
  el('tokRoot').value = '/';
  el('tokQuota').value = '0';
  el('tokMaxFile').value = '0';
  el('tokEnabled').checked = true;
  el('tokReadOnly').checked = false;
			el('tokDiskImages').value = '';
			el('tokDiskImagesWrite').value = '';
  el('tokDiskImagesAutoResize').value = '';
			el('tokDiskImagesRenameConvert').checked = false;
}

function tokSelect(){
  var cfg = tokReadConfig();
  if (!cfg || !cfg.tokens){
    tokEditIndex = -1;
    tokClearForm();
    return;
  }
  var idx = parseInt(el('tokSel').value || '-1', 10);
  if (!isFinite(idx) || idx < 0 || idx >= cfg.tokens.length){
    tokEditIndex = -1;
    tokClearForm();
    return;
  }
  tokEditIndex = idx;
  var t = cfg.tokens[idx] || {};
  el('tokName').value = t.name || '';
  el('tokToken').value = t.token || '';
  el('tokRoot').value = t.root || '/';
  el('tokQuota').value = String(t.quota_bytes || 0);
  el('tokMaxFile').value = String(t.max_file_bytes || 0);
  el('tokEnabled').checked = (t.enabled !== false);
  el('tokReadOnly').checked = (t.read_only === true);

  // Per-token disk image toggle (inherit / true / false)
  if (t.disk_images_enabled === true) el('tokDiskImages').value = 'true';
  else if (t.disk_images_enabled === false) el('tokDiskImages').value = 'false';
  else el('tokDiskImages').value = '';

  // Per-token disk image write toggle (inherit / true / false)
  if (t.disk_images_write_enabled === true) el('tokDiskImagesWrite').value = 'true';
  else if (t.disk_images_write_enabled === false) el('tokDiskImagesWrite').value = 'false';
  else el('tokDiskImagesWrite').value = '';

  // Per-token disk image auto-resize toggle (inherit / true / false)
  if (t.disk_images_auto_resize_enabled === true) el('tokDiskImagesAutoResize').value = 'true';
  else if (t.disk_images_auto_resize_enabled === false) el('tokDiskImagesAutoResize').value = 'false';
  else el('tokDiskImagesAutoResize').value = '';

  el('tokDiskImagesRenameConvert').checked = (t.disk_images_allow_rename_convert === true);
}

function tokNew(){
  tokEditIndex = -1;
  el('tokSel').value = '';
  tokClearForm();
}

function tokGen(){
  var bytes = new Uint8Array(16);
  if (window.crypto && crypto.getRandomValues){
    crypto.getRandomValues(bytes);
  } else {
    for (var i=0;i<bytes.length;i++) bytes[i] = Math.floor(Math.random()*256);
  }
  var hex = '';
  for (var j=0;j<bytes.length;j++){
    hex += ('0' + bytes[j].toString(16)).slice(-2);
  }
  el('tokToken').value = hex;
}

async function tokSave(){
  var cfg = tokReadConfig();
  if (!cfg){
    setStatus('Config JSON invalid; fix it first.', true);
    return;
  }
  if (!cfg.tokens) cfg.tokens = [];

  var token = (el('tokToken').value || '').trim();
  if (!token){
    setStatus('Token is required.', true);
    return;
  }

  var t = {
    token: token,
    name: (el('tokName').value || '').trim(),
    root: (el('tokRoot').value || '/').trim() || '/',
    enabled: el('tokEnabled').checked,
    read_only: el('tokReadOnly').checked,
    quota_bytes: parseInt(el('tokQuota').value || '0', 10) || 0,
    max_file_bytes: parseInt(el('tokMaxFile').value || '0', 10) || 0,
  };

  var di = el('tokDiskImages').value;
  if (di === 'true') t.disk_images_enabled = true;
  else if (di === 'false') t.disk_images_enabled = false;

  var diw = el('tokDiskImagesWrite').value;
  if (diw === 'true') t.disk_images_write_enabled = true;
  else if (diw === 'false') t.disk_images_write_enabled = false;

				var diar = el('tokDiskImagesAutoResize').value;
				if (diar === 'true') t.disk_images_auto_resize_enabled = true;
				else if (diar === 'false') t.disk_images_auto_resize_enabled = false;

				if (el('tokDiskImagesRenameConvert').checked) t.disk_images_allow_rename_convert = true;

				if (el('tokDiskImagesRenameConvert').checked) t.disk_images_allow_rename_convert = true;


  // Robust update: try index, then selected entry, then token match.
  var idx = tokEditIndex;
  if (!(idx >= 0 && idx < cfg.tokens.length)){
    var selIdx = parseInt(el('tokSel').value || '-1', 10);
    if (isFinite(selIdx) && selIdx >= 0 && selIdx < cfg.tokens.length) idx = selIdx;
  }
  if (!(idx >= 0 && idx < cfg.tokens.length)){
    for (var i=0; i<cfg.tokens.length; i++){
      var et = cfg.tokens[i] || {};
      if (String(et.token || '').trim() === token){ idx = i; break; }
    }
  }

  if (idx >= 0 && idx < cfg.tokens.length){
    cfg.tokens[idx] = t;
    tokEditIndex = idx;
  } else {
    cfg.tokens.push(t);
    tokEditIndex = cfg.tokens.length - 1;
  }

  // If duplicates exist (same token), keep the first/edited one.
  for (var j=cfg.tokens.length-1; j>=0; j--){
    if (j === tokEditIndex) continue;
    var jt = cfg.tokens[j] || {};
    if (String(jt.token || '').trim() === token){
      cfg.tokens.splice(j, 1);
      if (j < tokEditIndex) tokEditIndex--;
    }
  }

  tokWriteConfig(cfg);
  tokRefresh();
  el('tokSel').value = String(tokEditIndex);

  await saveConfig();
}

async function tokDelete(){
  var cfg = tokReadConfig();
  if (!cfg || !cfg.tokens){
    return;
  }
  var idx = parseInt(el('tokSel').value || '-1', 10);
  if (!isFinite(idx) || idx < 0 || idx >= cfg.tokens.length){
    return;
  }
  if (!confirm('Delete selected token from config?')){
    return;
  }
  cfg.tokens.splice(idx, 1);
  tokWriteConfig(cfg);
  tokNew();
  tokRefresh();
  await saveConfig();
}

async function tokRefreshAndKeep(){
  tokRefresh();
}

function tokRefreshBtn(){
  tokRefresh();
}

function fmtTime(ms){
  var d = new Date(ms);
  return d.toISOString().replace('T',' ').replace('Z',' UTC');
}

var charts = { req: null, bytes: null };
var logEntries = [];
var selectedLogID = null;
var selectedLogEntry = null;
var logEventSrc = null;

function setStatus(msg, cls){
  var s = el('statusMsg');
  s.textContent = msg || '';
  s.className = cls || 'small';
}

function toast(msg, cls, ttl){
  try{
    var w = el('toastWrap');
    if (!w) return;
    var d = document.createElement('div');
    d.className = 'toast ' + (cls || '');
    var p = document.createElement('div');
    p.textContent = msg || '';
    d.appendChild(p);
    w.appendChild(d);
    var ms = (typeof ttl==='number' && ttl>0) ? ttl : 2600;
    setTimeout(function(){
      d.style.transition = 'opacity 250ms';
      d.style.opacity = '0';
      setTimeout(function(){ try{ w.removeChild(d); } catch(e){} }, 300);
    }, ms);
  } catch(e){}
}

function flash(msg, cls){
  setStatus(msg, cls);
  var ttl = (cls==='bad') ? 5200 : (cls==='warn' ? 3200 : 2600);
  toast(msg, cls, ttl);
}

function setActionOut(obj){
  var p = el('actionOut').querySelector('pre');
  if (typeof obj === 'string') p.textContent = obj;
  else p.textContent = JSON.stringify(obj, null, 2);
}

async function jget(url){
  try{
    var res = await fetch(url, {cache:'no-store'});
    var txt = await res.text();
    try { return [res.ok, JSON.parse(txt)]; } catch(e){ return [res.ok, txt]; }
  } catch(e){
    return [false, {message: String(e)}];
  }
}

async function jpost(url, bodyObj){
  try{
    var res = await fetch(url, {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: bodyObj ? JSON.stringify(bodyObj) : '{}'
    });
    var txt = await res.text();
    try { return [res.ok, JSON.parse(txt)]; } catch(e){ return [res.ok, txt]; }
  } catch(e){
    return [false, {message: String(e)}];
  }
}

function renderWarnings(warns){
  var w = el('warnings');
  w.innerHTML = '';
  if (!warns || warns.length===0){
    w.innerHTML = '<span class="badge good">OK</span> <span class="small">No config warnings</span>';
    return;
  }
  var ul = document.createElement('ul');
  ul.style.margin = '6px 0 0 18px';
  for (var i=0; i<warns.length; i++){
    var li = document.createElement('li');
    li.className = 'warn';
    li.textContent = warns[i];
    ul.appendChild(li);
  }
  w.appendChild(document.createElement('hr'));
  w.appendChild(ul);
}

function initCharts(){
  if (!window.Chart) return;
  var ctx1 = el('chartReq').getContext('2d');
  charts.req = new Chart(ctx1, {
    type: 'line',
    data: { labels: [], datasets: [
      { label:'Requests/min', data: [], tension:0.2, borderWidth:2 },
      { label:'Errors/min', data: [], tension:0.2, borderWidth:2 }
    ]},
    options: {
      responsive:true,
      maintainAspectRatio:false,
      plugins:{ legend:{ labels:{ color:'#ffffff' } } },
      scales:{
        x:{ ticks:{ color:'#a9b8ff' }, grid:{ color:'rgba(169,184,255,0.15)' } },
        y:{ ticks:{ color:'#a9b8ff' }, grid:{ color:'rgba(169,184,255,0.15)' } }
      }
    }
  });

  var ctx2 = el('chartBytes').getContext('2d');
  charts.bytes = new Chart(ctx2, {
    type: 'line',
    data: { labels: [], datasets: [
      { label:'Bytes In/min (KiB)', data: [], tension:0.2, borderWidth:2 },
      { label:'Bytes Out/min (KiB)', data: [], tension:0.2, borderWidth:2 }
    ]},
    options: {
      responsive:true,
      maintainAspectRatio:false,
      plugins:{ legend:{ labels:{ color:'#ffffff' } } },
      scales:{
        x:{ ticks:{ color:'#a9b8ff' }, grid:{ color:'rgba(169,184,255,0.15)' } },
        y:{ ticks:{ color:'#a9b8ff' }, grid:{ color:'rgba(169,184,255,0.15)' } }
      }
    }
  });
}

function updateCharts(stats){
  if (!charts.req || !charts.bytes) return;
  var pts = stats.recent || [];
  var labels = [];
  var req = [];
  var err = [];
  var bin = [];
  var bout = [];
  for (var i=0; i<pts.length; i++){
    var t = new Date((pts[i].minute_unix||0)*1000);
    labels.push(t.toISOString().substring(11,16));
    req.push(Number(pts[i].requests||0));
    err.push(Number(pts[i].errors||0));
    bin.push(Math.round(Number(pts[i].bytes_in||0)/1024));
    bout.push(Math.round(Number(pts[i].bytes_out||0)/1024));
  }

  charts.req.data.labels = labels;
  charts.req.data.datasets[0].data = req;
  charts.req.data.datasets[1].data = err;
  charts.req.update('none');

  charts.bytes.data.labels = labels;
  charts.bytes.data.datasets[0].data = bin;
  charts.bytes.data.datasets[1].data = bout;
  charts.bytes.update('none');
}

function renderTokens(data){
  el('tokenMeta').textContent = 'Updated: ' + new Date().toLocaleTimeString();
  var tb = el('tokenTable').querySelector('tbody');
  tb.innerHTML = '';
  var list = data.tokens || [];
  for (var i=0; i<list.length; i++){
    var t = list[i];
    var tr = document.createElement('tr');

    var flags = [];
    if (t.read_only) flags.push('RO');
    if (t.ignored) flags.push('IGNORED');
    if (t.enabled === false) flags.push('DISABLED');

    var q = t.quota_bytes ? fmtBytes(t.quota_bytes) : '-';
    var used = (t.used_bytes !== undefined) ? fmtBytes(t.used_bytes) : '-';
    var tmp = (t.tmp_bytes !== undefined) ? fmtBytes(t.tmp_bytes) : '-';

    var usedBadge = '';
    if (t.quota_bytes && t.used_pct !== undefined){
      if (t.used_pct >= 100) usedBadge = '<span class="badge bad">FULL</span> ';
      else if (t.used_pct >= 90) usedBadge = '<span class="badge warn">' + t.used_pct + '%</span> ';
      else usedBadge = '<span class="badge good">' + t.used_pct + '%</span> ';
    }

    var tokenCell = (t.token_mask || '');
    if (t.token_id) tokenCell += '<div class="small">#' + t.token_id + '</div>';

    var flagsCell = '';
    for (var f=0; f<flags.length; f++){
      flagsCell += '<span class="badge">' + flags[f] + '</span> ';
    }

    var quotaCell = q;
    if (t.max_file_bytes) quotaCell += '<div class="small">max_file=' + fmtBytes(t.max_file_bytes) + '</div>';

    var usedCell = usedBadge + used;
    if (t.remaining_bytes !== undefined) usedCell += '<div class="small">left=' + fmtBytes(t.remaining_bytes) + '</div>';
    if (t.error) usedCell += '<div class="bad small">' + t.error + '</div>';

    tr.innerHTML =
      '<td>' + (t.kind||'') + '</td>' +
      '<td>' + (t.name||'') + '</td>' +
      '<td>' + tokenCell + '</td>' +
      '<td><div class="small">' + (t.root_abs||'') + '</div></td>' +
      '<td>' + flagsCell + '</td>' +
      '<td>' + quotaCell + '</td>' +
      '<td>' + usedCell + '</td>' +
      '<td>' + tmp + '</td>';

    tb.appendChild(tr);
  }
}

function renderStats(st){
  el('uptime').textContent = st.uptime_sec ? (st.uptime_sec + ' s') : '-';
  el('statReq').textContent = (st.total_requests !== undefined) ? String(st.total_requests) : '-';
  el('statErr').textContent = (st.total_errors !== undefined) ? String(st.total_errors) : '-';
  el('statAvg').textContent = (st.avg_ms !== undefined) ? String(st.avg_ms) : '-';
  el('statIn').textContent = (st.bytes_in !== undefined) ? fmtBytes(st.bytes_in) : '-';
  el('statOut').textContent = (st.bytes_out !== undefined) ? fmtBytes(st.bytes_out) : '-';
  updateCharts(st);
}

function renderConfig(data){
  el('cfgText').value = JSON.stringify(data.config, null, 2);

      if (data.build && data.build.version){
        el('build').textContent = data.build.version + ' (' + data.build.commit + ', ' + data.build.build_date + ')';
        // Header subtitle is kept clean/retro. Build info is available via tooltip + build tile.
        el('hdrSub').textContent = 'Backend @ ' + (data.listen||'?') + (data.endpoint||'');
      } else {
    el('build').textContent = JSON.stringify(data.build);
    el('hdrSub').textContent = 'Backend @ ' + (data.listen||'?') + (data.endpoint||'');
  }

  // Logo tooltip (top-right).
  var ver = (data.build && data.build.version) ? data.build.version : '';
  var logo = el('logoImg');
  if (logo){
        logo.title = 'WiCOS64 Remote Storage Server\nVersion ' + (ver||'?') + '\nRetroBytes / Forum64.de';
  }

  el('listen').textContent = data.listen || '-';
  el('endpoint').textContent = data.endpoint || '-';
  renderWarnings(data.warnings || []);
  el('cfgMeta').textContent = data.config_path ? ('config: ' + data.config_path) : '';

  // Extra status tiles
  renderBootstrap(data.config);
  renderTrash(data.config);
  tokRefresh();
  cfgSyncFormFromText(false);
}

function logLineText(e){
  var t = new Date(e.time_unix_ms || 0);
  var hh = t.toISOString().substring(11,19);
  var ip = e.remote_ip || '-';
  var op = e.op_name || '??';
  var st = e.status_name || '';
  var ms = (e.duration_ms !== undefined) ? (e.duration_ms + 'ms') : '';
  var info = e.info || '';
  return hh + ' ' + ip + ' ' + op + ' ' + st + ' ' + ms + ' ' + info;
}

function renderLogList(){
  var list = el('logList');
  list.innerHTML = '';
  for (var i=0; i<logEntries.length; i++){
    var e = logEntries[i];
    var d = document.createElement('div');
    d.className = 'logline' + (e.id === selectedLogID ? ' active' : '');
    d.textContent = logLineText(e);
    d.onclick = (function(id){ return function(){ selectLog(id); }; })(e.id);
    list.appendChild(d);
  }
  el('logMeta').textContent = logEntries.length + ' entries';
}

function selectLog(id){
  selectedLogID = id;
  renderLogList();

  var found = null;
  for (var i=0; i<logEntries.length; i++){
    if (logEntries[i].id === id){ found = logEntries[i]; break; }
  }
  if (!found){
    el('logDetail').textContent = '(not found)';
    selectedLogEntry = null;
    if (el('logSelMeta')) el('logSelMeta').textContent = '(no selection)';
    if (el('logToOpsBtn')) el('logToOpsBtn').disabled = true;
    return;
  }

  selectedLogEntry = found;
  if (el('logSelMeta')){
    var opn = (found.op_name||'').trim();
    var ip = (found.remote_ip||'').trim();
    el('logSelMeta').textContent = 'Selected: #' + found.id + (opn ? (' · ' + opn) : '') + (ip ? (' · ' + ip) : '');
  }
  if (el('logToOpsBtn')) el('logToOpsBtn').disabled = false;

  var opHex = (found.op||0).toString(16).toUpperCase();
  if (opHex.length < 2) opHex = '0' + opHex;
  var stHex = (found.status||0).toString(16).toUpperCase();
  if (stHex.length < 2) stHex = '0' + stHex;

  var parts = [];
  parts.push('LOG #' + found.id);
  parts.push('time: ' + fmtTime(found.time_unix_ms||0));
  parts.push('ip: ' + (found.remote_ip||''));
  parts.push('op: ' + (found.op_name||'') + ' (0x' + opHex + ')');
  parts.push('status: ' + (found.status_name||'') + ' (0x' + stHex + ')');
  parts.push('http: ' + (found.http_status||''));
  parts.push('req_bytes: ' + (found.req_bytes||0) + '   resp_bytes: ' + (found.resp_bytes||0) + '   dur_ms: ' + (found.duration_ms||0));
  parts.push('info: ' + (found.info||''));
  parts.push('');
  parts.push('--- REQUEST ---');
  parts.push(found.req_preview || '(no preview)');
  parts.push('');
  parts.push('--- RESPONSE ---');
  parts.push(found.resp_preview || '(no preview)');

  el('logDetail').textContent = parts.join('\n');
}

// Send selected log entry into Ops Playground
function splitInfoTokens(s){
  s = (s || '').trim();
  if(!s) return [];
  var out = [];
  var cur = '';
  var inQ = false;
  var esc = false;
  for(var i=0;i<s.length;i++){
    var ch = s[i];
    if(esc){
      cur += ch;
      esc = false;
      continue;
    }
    if(ch === '\\'){
      // keep backslash so quoted strings remain valid for the CLI parser
      cur += ch;
      esc = true;
      continue;
    }
    if(ch === '"'){
      inQ = !inQ;
      cur += ch;
      continue;
    }
    if(!inQ && (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r')){
      if(cur){ out.push(cur); cur=''; }
      continue;
    }
    cur += ch;
  }
  if(cur) out.push(cur);
  return out;
}

function parseInfoKV(s){
  var kv = {};
  var toks = splitInfoTokens(s);
  for(var i=0;i<toks.length;i++){
    var t = toks[i];
    var eq = t.indexOf('=');
    if(eq <= 0) continue;
    var k = t.substring(0, eq);
    var v = t.substring(eq+1);
    kv[k] = v;
  }
  return kv;
}

function hexByte(n){
  n = (n>>>0) & 0xFF;
  return '0x' + n.toString(16).toUpperCase().padStart(2,'0');
}

function opsLineFromLog(e){
  if(!e) return '';
  var op = (e.op||0) & 0xFF;
  var kv = parseInfoKV(e.info||'');

  var path = kv.path || '"/"';
  var src = kv.src || '';
  var dst = kv.dst || '';
  var off = kv.off || '0';
  var len = kv.len || '0';
  var start = kv.start || '0';
  var max = kv.max || '200';
  var scan = kv.scan || '500';
  var q = kv.q || '""';

  var flags = (kv.flags || '');

  // normalize flags list like OVERWRITE|RECURSIVE
  var fset = {};
  if(flags){
    flags.split('|').forEach(function(x){
      x = (x||'').trim();
      if(x) fset[x.toUpperCase()] = true;
    });
  }

  switch(op){
    case 0x0E: return 'caps';
    case 0x0D: return 'ping';
    case 0x0F: return 'statfs ' + path;
    case 0x01: {
      var line = 'ls ' + path;
      // only add range if present in info
      if(kv.start !== undefined || kv.max !== undefined){
        line += ' ' + start + ' ' + max;
      }
      return line;
    }
    case 0x02: return 'stat ' + path;
    case 0x03: return 'read ' + path + ' ' + off + ' ' + len;
    case 0x04: {
      var opts = '';
      if(fset['CREATE']) opts += ' -c';
      if(fset['TRUNCATE']) opts += ' -t';
      return 'write' + opts + ' ' + path + ' ' + off;
    }
    case 0x05: {
      var opts = '';
      if(fset['CREATE']) opts += ' -c';
      return 'append' + opts + ' ' + path;
    }
    case 0x06: {
      var opts = '';
      if(fset['PARENTS']) opts += ' -p';
      return 'mkdir' + opts + ' ' + path;
    }
    case 0x07: {
      var opts = '';
      if(fset['RECURSIVE']) opts += ' -r';
      return 'rmdir' + opts + ' ' + path;
    }
    case 0x08: return 'rm ' + path;
    case 0x09: {
      var opts = '';
      if(fset['OVERWRITE']) opts += ' -o';
      if(fset['RECURSIVE']) opts += ' -r';
      if(!src) src = kv.from || kv.src_path || '';
      if(!dst) dst = kv.to || kv.dst_path || '';
      if(!src) src = '"/"';
      if(!dst) dst = '"/"';
      return 'cp' + opts + ' ' + src + ' ' + dst;
    }
    case 0x0A: {
      var opts = '';
      if(fset['OVERWRITE']) opts += ' -o';
      if(!src) src = kv.from || kv.src_path || '';
      if(!dst) dst = kv.to || kv.dst_path || '';
      if(!src) src = '"/"';
      if(!dst) dst = '"/"';
      return 'mv' + opts + ' ' + src + ' ' + dst;
    }
    case 0x0B: {
      // SEARCH flags: CI|RECURSIVE|WHOLE
      var sflags = 0;
      if(fset['CI'] || fset['CASE_INSENSITIVE']) sflags |= 1;
      if(fset['RECURSIVE']) sflags |= 2;
      if(fset['WHOLE'] || fset['WHOLE_WORD']) sflags |= 4;

      var line = 'search ' + (kv.base || path) + ' ' + q;
      if(kv.start !== undefined || kv.max !== undefined || kv.scan !== undefined){
        line += ' ' + start + ' ' + max + ' ' + scan;
      }
      if(sflags){
        line += ' -f ' + hexByte(sflags);
      }
      return line;
    }
    case 0x0C: {
      var hflags = 0;
      if((kv.algo||'').toUpperCase() === 'SHA1') hflags |= 1;
      var line = 'hash ' + path;
      if(hflags){
        line += ' -f ' + hexByte(hflags);
      }
      return line;
    }
  }

  // Fallback: map by op_name if available
  var opName = (e.op_name||'').toLowerCase();
  if(opName){
    if(opName === 'ping') return 'ping';
    if(opName === 'caps') return 'caps';
    if(opName === 'statfs') return 'statfs ' + path;
    if(opName === 'ls') return 'ls ' + path;
    if(opName === 'stat') return 'stat ' + path;
    if(opName === 'read') return 'read ' + path + ' ' + off + ' ' + len;
    if(opName === 'rm') return 'rm ' + path;
  }
  return '';
}

function logToOps(){
  if(!selectedLogEntry){
    toast('Select a log entry first', 'warn');
    return;
  }
  var line = opsLineFromLog(selectedLogEntry);
  if(!line){
    toast('Could not convert selected log to a playground command', 'warn');
    return;
  }

  // Open playground
  if(el('opsDetails')) el('opsDetails').open = true;

  // Try to match token by name (best-effort)
  try{
    if(selectedLogEntry.token_hint && window.opsTokenByName){
      var k = String(selectedLogEntry.token_hint||'').trim();
      if(k && opsTokenByName[k] && el('opsToken')){
        el('opsToken').value = opsTokenByName[k];
      }
    }
  }catch(e){}

  el('opsCmd').value = line;
  el('opsCmd').focus();
  el('opsCmd').setSelectionRange(el('opsCmd').value.length, el('opsCmd').value.length);

  // Writes/appends cannot reconstruct the data (we only store previews)
  var op = (selectedLogEntry.op||0) & 0xFF;
  if(op === 0x04 || op === 0x05){
    toast('Note: write/append data is not captured in logs. Paste payload in the Data box.', 'warn', 3800);
  }else{
    toast('Loaded into Ops Playground', 'ok', 1600);
  }
}

function currentLogFilter(){
  var op = el('fOp').value.trim();
  var errs = el('fErr').checked ? '1' : '';
  var ip = el('fIP').value.trim();
  var q = el('fQ').value.trim();
  var limit = el('fLimit').value.trim() || '300';

  var params = new URLSearchParams();
  if (op) params.set('op', op);
  if (errs) params.set('errors', errs);
  if (ip) params.set('ip', ip);
  if (q) params.set('q', q);
  if (limit) params.set('limit', limit);
  return params.toString();
}

async function reloadLogs(){
  var qs = currentLogFilter();
  var r = await jget('/admin/api/logs?' + qs);
  var ok = r[0];
  var data = r[1];
  if (!ok){ setStatus('log load failed', 'bad'); return; }
  logEntries = data || [];
  if (logEntries.length > 0){
    selectedLogID = logEntries[logEntries.length-1].id;
  }
  renderLogList();
  if (selectedLogID) selectLog(selectedLogID);
}

function startLogStream(){
  if (logEventSrc){ try{ logEventSrc.close(); } catch(e){} }
  logEventSrc = new EventSource('/admin/stream/logs');

  logEventSrc.onmessage = function(ev){
    try{
      var e = JSON.parse(ev.data);
      var qs = currentLogFilter();
      var params = new URLSearchParams(qs);

      if (params.get('errors')==='1' && e.status===0) return;
      var op = params.get('op');
      if (op){ var n = parseInt(op,16); if (e.op !== n) return; }
      var ip = params.get('ip');
      if (ip && String(e.remote_ip||'').indexOf(ip) === -1) return;
      var q = params.get('q');
      if (q){
        var hay = String(e.info||'') + '\n' + String(e.req_preview||'') + '\n' + String(e.resp_preview||'');
        if (hay.toLowerCase().indexOf(q.toLowerCase()) === -1) return;
      }

      logEntries.push(e);
      var lim = parseInt(params.get('limit') || '300', 10);
      while (logEntries.length > lim) logEntries.shift();
      renderLogList();

    } catch(err){
      // ignore
    }
  };

  logEventSrc.onerror = function(){
    el('logMeta').textContent = 'stream: reconnecting…';
  };
}

// ---- OPS PLAYGROUND ------------------------------------------------------
function opsSetTokens(data){
  var sel = el('opsToken');
  if(!sel) return;
  sel.innerHTML = '';

  window.opsTokenByName = {};

  var list = (data && data.tokens) ? data.tokens : [];
  var added = 0;

  list.forEach(function(t){
    if(!t || !t.enabled) return;
    if(t.ignored) return;
    var kind = t.kind || '';
    var tid  = t.token_id || '';
    var label = '';
    if(kind === 'no_auth'){
      label = 'NO AUTH';
    }else{
      label = (t.name ? (t.name + ' — ') : '') + (t.token_mask || (kind + ':' + tid));
    }

    var opt = document.createElement('option');
    opt.value = kind + ':' + tid;
    opt.textContent = label;
    sel.appendChild(opt);
    added++;

    if(t.name){
      window.opsTokenByName[String(t.name).trim()] = opt.value;
    }
  });

  if(added === 0){
    var opt = document.createElement('option');
    opt.value = 'no_auth:';
    opt.textContent = '(no usable tokens)';
    sel.appendChild(opt);
  }
}

function opsClear(){
  var out = el('opsOut');
  if(out) out.textContent = '';
  var meta = el('opsMeta');
  if(meta) meta.textContent = '';
}

function opsAppend(s){
  var out = el('opsOut');
  if(!out) return;
  if(out.textContent && !out.textContent.endsWith('\n')) out.textContent += '\n';
  out.textContent += s;
  out.scrollTop = out.scrollHeight;
}

async function opsRun(){
  var cmdEl = el('opsCmd');
  if(!cmdEl) return;

  var line = (cmdEl.value || '').trim();
  if(!line){
    toast('Enter a command (e.g. ls /)', 'warn');
    cmdEl.focus();
    return;
  }

  // ---- history ----------------------------------------------------------
  try{
    opsHistPush(line);
  }catch(e){}

  var tokVal = (el('opsToken') && el('opsToken').value) ? el('opsToken').value : 'no_auth:';
  var parts = tokVal.split(':');
  var token_kind = parts[0] || '';
  var token_id   = parts.slice(1).join(':');

  var data = (el('opsData') && el('opsData').value) ? el('opsData').value : '';
  var data_enc = (el('opsDataEnc') && el('opsDataEnc').value) ? el('opsDataEnc').value : 'text';

  var meta = el('opsMeta');
  if(meta) meta.textContent = 'running...';

  var t0 = performance.now();
  var r = await jpost('/admin/api/ops/run', { token_kind: token_kind, token_id: token_id, line: line, data: data, data_enc: data_enc });
  var dt = Math.round(performance.now() - t0);

  if(meta) meta.textContent = '';

  if(!r[0]){
    var msg = (r[1] && r[1].error) ? r[1].error : JSON.stringify(r[1]);
    opsAppend('[' + new Date().toLocaleTimeString() + '] ' + line + '\nERROR: ' + msg + '\n');
    toast('Ops failed: ' + msg, 'bad');
    return;
  }

  var res = r[1] || {};
  var head = '[' + new Date().toLocaleTimeString() + '] ' + line;
  if(res.op){
    head += '  (' + res.op + (typeof res.op_code === 'number' ? ('/0x' + res.op_code.toString(16).padStart(2,'0')) : '') + ')';
  }

  var st = (res.status || '');
  if(res.status_code !== undefined && res.status_code !== null){
    st += ' (' + res.status_code + ')';
  }

  opsAppend(head + '\nSTATUS: ' + st + '  in ' + (res.duration_ms||dt) + ' ms');

  if(res.err_msg){
    opsAppend('ERRMSG: ' + res.err_msg);
  }

  if(res.pretty){
    opsAppend((res.pretty || '').trimEnd());
  }

  if(res.resp_bytes !== undefined){
    opsAppend('RESP BYTES: ' + res.resp_bytes);
  }

  opsAppend('');

  if(res.status_code === 0){
    toast('OK', 'ok');
  }else{
    toast('Status: ' + (res.status||res.status_code), 'warn');
  }
}
// -------------------------------------------------------------------------

// ---- OPS HISTORY ---------------------------------------------------------
var opsHist = [];
var opsHistIdx = -1;
var opsHistDraft = '';

function opsHistLoad(){
  try{
    var raw = localStorage.getItem('wicos_ops_history') || '';
    if(raw){
      var arr = JSON.parse(raw);
      if(Array.isArray(arr)) opsHist = arr.filter(function(x){ return typeof x==='string' && x.trim(); });
    }
  }catch(e){ opsHist = []; }
  if(opsHist.length > 80) opsHist = opsHist.slice(opsHist.length-80);
  opsHistIdx = -1;
  opsHistDraft = '';
}

function opsHistSave(){
  try{
    localStorage.setItem('wicos_ops_history', JSON.stringify(opsHist.slice(-80)));
  }catch(e){}
}

function opsHistPush(line){
  line = (line||'').trim();
  if(!line) return;
  var last = opsHist.length ? opsHist[opsHist.length-1] : '';
  if(last === line){
    opsHistIdx = -1;
    opsHistDraft = '';
    return;
  }
  opsHist.push(line);
  if(opsHist.length > 80) opsHist = opsHist.slice(opsHist.length-80);
  opsHistSave();
  opsHistIdx = -1;
  opsHistDraft = '';
}

function opsHistNav(dir){
  var cmdEl = el('opsCmd');
  if(!cmdEl) return;
  if(!opsHist || opsHist.length === 0) return;

  if(opsHistIdx === -1){
    opsHistDraft = cmdEl.value || '';
    opsHistIdx = opsHist.length;
  }

  opsHistIdx += dir;
  if(opsHistIdx < 0) opsHistIdx = 0;
  if(opsHistIdx > opsHist.length) opsHistIdx = opsHist.length;

  if(opsHistIdx === opsHist.length){
    cmdEl.value = opsHistDraft;
  }else{
    cmdEl.value = opsHist[opsHistIdx];
  }
  cmdEl.focus();
  cmdEl.setSelectionRange(cmdEl.value.length, cmdEl.value.length);
}
// -------------------------------------------------------------------------

async function loadConfig(){
  setStatus('loading config...', 'warn');
  flash('Reloading config from server...', 'warn');
  // NOTE: config endpoint is /admin/api/config (GET returns effective config + metadata).
  var r = await jget('/admin/api/config');
  if (!r[0]) {
    setStatus('config load failed', 'bad');
    var msg = (r[1] && r[1].error) ? r[1].error : JSON.stringify(r[1]);
    flash('Config reload failed: ' + msg, 'bad');
    return;
  }
  renderConfig(r[1]);
  setStatus('config loaded', 'good');
  flash('Config loaded from server.', 'good');
}

async function saveConfig(){
  var obj = null;
  try { obj = JSON.parse(el('cfgText').value); } catch(e){ flash('config JSON invalid: ' + e, 'bad'); return; }
  setStatus('saving config…', 'warn');
  var r = await jpost('/admin/api/config', obj);
  if (!r[0]){ flash('config save failed: ' + (r[1].message || r[1]), 'bad'); return; }
  flash('config saved', 'good');
  renderWarnings(r[1].warnings || []);
  await loadConfig();
  await loadTokens();
}

async function loadTokens(){
  var r = await jget('/admin/api/tokens');
  if (!r[0]){ setStatus('tokens load failed', 'bad'); return; }
  renderTokens(r[1]);
  opsSetTokens(r[1]);
}

async function loadStats(){
  var r = await jget('/admin/api/stats');
  if (!r[0]) return;
  renderStats(r[1]);
}

async function actionReload(){
  setStatus('reloading config…', 'warn');
  toast('Reloading config…', 'warn', 1200);
  var r = await jpost('/admin/api/reload', null);
  setActionOut(r[1]);
  flash(r[0] ? 'config reloaded' : 'reload failed', r[0] ? 'good' : 'bad');
  await loadConfig();
  await loadTokens();
}

async function actionCleanup(){
  setStatus('running cleanup…', 'warn');
  toast('Running cleanup…', 'warn', 1200);
  var r = await jpost('/admin/api/cleanup/run', null);
  setActionOut(r[1]);
  flash(r[0] ? 'cleanup done' : 'cleanup failed', r[0] ? 'good' : 'bad');
  await loadTokens();
}

async function actionSelfTest(){
  setStatus('running self-test…', 'warn');
  toast('Running self-test…', 'warn', 1200);
  var r = await jpost('/admin/api/selftest', null);
  setActionOut(r[1]);
  flash(r[0] ? 'self-test done' : 'self-test failed', r[0] ? 'good' : 'bad');
}

async function actionStatsReset(){
  setStatus('resetting stats…', 'warn');
  toast('Resetting stats…', 'warn', 1200);
  var r = await jpost('/admin/api/stats/reset', null);
  setActionOut(r[1]);
  flash(r[0] ? 'stats reset' : 'stats reset failed', r[0] ? 'good' : 'bad');
  await loadStats();
}

async function actionLogsClear(){
  if (!confirm('Clear in-memory logs now?')) return;
  setStatus('clearing logs…', 'warn');
  toast('Clearing logs…', 'warn', 1200);
  var r = await jpost('/admin/api/logs/clear', null);
  setActionOut(r[1]);
  flash(r[0] ? 'logs cleared' : 'logs clear failed', r[0] ? 'good' : 'bad');
  await reloadLogs();
}

function actionLogsExport(){
  window.location = '/admin/api/logs/export?' + currentLogFilter();
}

async function boot(){
  initCharts();
  cfgInitForm();
  opsHistLoad();
  await loadConfig();
  await loadTokens();
  await loadStats();
  await reloadLogs();
  startLogStream();

  if(el("opsCmd")){
    el("opsCmd").addEventListener("keydown", function(ev){
      if(ev.key==="Enter"){
        ev.preventDefault();
        opsRun();
        return;
      }
      if(ev.key === 'ArrowUp'){
        ev.preventDefault();
        opsHistNav(-1);
        return;
      }
      if(ev.key === 'ArrowDown'){
        ev.preventDefault();
        opsHistNav(+1);
        return;
      }
    });
  }
  setInterval(loadStats, 2000);
  setInterval(loadTokens, 10000);
}

boot();
</script>
</body>
</html>
`

func (s *Server) mountAdmin(mux *http.ServeMux) {
	// UI page.
	mux.HandleFunc(adminPath, s.requireAdmin(s.handleAdminIndex))
	// Static (offline assets).
	mux.HandleFunc(adminPath+"/static/chart.umd.min.js", s.requireAdmin(s.handleAdminChartJS))
	mux.HandleFunc(adminPath+"/static/logo.svg", s.requireAdmin(s.handleAdminLogoSVG))
	// API.
	mux.HandleFunc(adminPath+"/api/config", s.requireAdmin(s.handleAdminConfig))
	// Backward-compatible alias (older Admin UI builds called /api/config/get).
	mux.HandleFunc(adminPath+"/api/config/get", s.requireAdmin(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.handleAdminConfig(w, r)
	}))
	mux.HandleFunc(adminPath+"/api/stats", s.requireAdmin(s.handleAdminStats))
	mux.HandleFunc(adminPath+"/api/stats/reset", s.requireAdmin(s.handleAdminStatsReset))
	mux.HandleFunc(adminPath+"/api/reload", s.requireAdmin(s.handleAdminReload))
	mux.HandleFunc(adminPath+"/api/shutdown", s.requireAdmin(s.handleAdminShutdown))
	mux.HandleFunc(adminPath+"/api/cleanup/run", s.requireAdmin(s.handleAdminCleanupRun))
	mux.HandleFunc(adminPath+"/api/selftest", s.requireAdmin(s.handleAdminSelfTest))
	mux.HandleFunc(adminPath+"/api/tokens", s.requireAdmin(s.handleAdminTokens))
	mux.HandleFunc(adminPath+"/api/logs", s.requireAdmin(s.handleAdminLogs))
	mux.HandleFunc(adminPath+"/api/logs/export", s.requireAdmin(s.handleAdminLogsExport))
	mux.HandleFunc(adminPath+"/api/ops/run", s.requireAdmin(s.handleAdminOpsRun))
	mux.HandleFunc(adminPath+"/api/logs/clear", s.requireAdmin(s.handleAdminLogsClear))
	// Stream.
	mux.HandleFunc(adminPath+"/stream/logs", s.requireAdmin(s.handleAdminLogStream))
}

func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg := s.cfgSnapshot()
		if !cfg.EnableAdminUI {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !cfg.AdminAllowRemote {
			ip := clientIP(r)
			if ip == "" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			parsed := net.ParseIP(ip)
			if parsed == nil || !parsed.IsLoopback() {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("admin UI is localhost-only by default\n"))
				return
			}
		}
		// Optional basic auth.
		if cfg.AdminPassword != "" {
			u, p, ok := r.BasicAuth()
			if !ok || u != cfg.AdminUser || p != cfg.AdminPassword {
				w.Header().Set("WWW-Authenticate", `Basic realm="WiCOS64 Admin"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) handleAdminIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != adminPath {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(adminHTML))
}

func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	// GET: return effective config.
	// POST: validate + save config.json (+ update running config, excluding listen/endpoint routing).
	cfg := s.cfgSnapshot()

	switch r.Method {
	case http.MethodGet:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		// Build an example URL that includes the listen port (useful when Listen is ":8080").
		apiHostPort := "127.0.0.1"
		if host, port, err := net.SplitHostPort(cfg.Listen); err == nil {
			// If Listen binds to all interfaces, still show a localhost example.
			if host != "" && host != "0.0.0.0" && host != "::" {
				apiHostPort = host
			}
			apiHostPort = apiHostPort + ":" + port
		} else {
			// Fallback: append Listen as-is.
			apiHostPort = apiHostPort + cfg.Listen
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"build":           version.Get(),
			"listen":          cfg.Listen,
			"endpoint":        cfg.Endpoint,
			"api_url_example": fmt.Sprintf("http://%s%s?token=%s", apiHostPort, cfg.Endpoint, "CHANGE-ME"),
			"config":          cfg,
			"warnings":        configWarnings(cfg),
			"config_path":     s.cfgPath,
		})
		return

	case http.MethodPost:
		if s.cfgPath == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("server was started without -config path; cannot save\n"))
			return
		}
		var posted config.Config
		// Merge into defaults, like config.Load().
		posted = config.Default()
		dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20)) // 1MB should be more than enough
		if err := dec.Decode(&posted); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid json: " + err.Error() + "\n"))
			return
		}
		if err := posted.Validate(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("invalid config: " + err.Error() + "\n"))
			return
		}
		// Update runtime config, but keep current listen/endpoint routing to avoid confusion.
		runtime := posted
		runtime.Listen = cfg.Listen
		runtime.Endpoint = cfg.Endpoint
		s.setCfg(runtime)
		// Save to disk (as posted, so listen/endpoint changes are persisted for next restart).
		if err := saveConfigJSON(s.cfgPath, posted); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("failed to save: " + err.Error() + "\n"))
			return
		}
		restartRequired := posted.Listen != cfg.Listen || posted.Endpoint != cfg.Endpoint
		resp := map[string]any{
			"ok":               true,
			"warnings":         configWarnings(runtime),
			"restart_required": restartRequired,
			"note":             "Saved. Restart required for listen/endpoint changes.",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
		return
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
}

func saveConfigJSON(path string, cfg config.Config) error {
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	// Ensure trailing newline for nicer diffs.
	b = append(b, '\n')
	// Best-effort backup of the previous config to help recover from accidental overwrites.
	// (This intentionally does not fail the save if the backup cannot be written.)
	if old, rerr := os.ReadFile(path); rerr == nil {
		_ = os.WriteFile(path+".bak", old, 0o644)
	}
	return writeFileAtomic(path, b, 0o644)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "wicos64-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		// Best effort cleanup if anything goes wrong.
		_ = os.Remove(tmpName)
	}()
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func parseLogFilter(r *http.Request) LogFilter {
	q := r.URL.Query()
	f := LogFilter{Limit: 200}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			if n < 1 {
				n = 1
			}
			if n > 5000 {
				n = 5000
			}
			f.Limit = n
		}
	}
	if v := q.Get("op"); v != "" {
		// allow hex like "0E" or decimal.
		if b, err := parseByteAuto(v); err == nil {
			t := b
			f.Op = &t
		}
	}
	if q.Get("errors") == "1" {
		f.OnlyErrors = true
	}
	f.RemoteIPSub = q.Get("ip")
	f.InfoContains = q.Get("q")
	if v := q.Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.SinceUnixMs = n
		}
	}
	if v := q.Get("until"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			f.UntilUnixMs = n
		}
	}
	return f
}

func parseByteAuto(s string) (byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty")
	}
	// hex?
	if len(s) <= 2 {
		if n, err := strconv.ParseUint(s, 16, 8); err == nil {
			return byte(n), nil
		}
	}
	// decimal
	n, err := strconv.ParseUint(s, 10, 8)
	return byte(n), err
}

func formatLogLine(e LogEntry) string {
	ts := time.UnixMilli(e.TimeUnixMs).UTC().Format(time.RFC3339Nano)
	line := fmt.Sprintf("[%s] %s %s %s req=%d resp=%d %dms", ts, e.RemoteIP, e.OpName, e.StatusName, e.ReqBytes, e.RespBytes, e.DurationMs)
	if e.Info != "" {
		line += " | " + e.Info
	}
	if e.HTTPStatus != 0 && e.HTTPStatus != 200 {
		line += fmt.Sprintf(" | http=%d", e.HTTPStatus)
	}
	return line
}

func configWarnings(cfg config.Config) []string {
	var w []string

	hasAuth := cfg.Token != "" || len(cfg.TokenRoots) > 0 || len(cfg.Tokens) > 0
	if !hasAuth {
		w = append(w, "no token configured: server runs in no-auth mode (NOT RECOMMENDED)")
	} else if cfg.Token != "" && strings.EqualFold(cfg.Token, "CHANGE-ME") {
		w = append(w, "token is still CHANGE-ME – please set a real secret")
	}
	if cfg.BasePath == "" {
		w = append(w, "base_path is empty")
	} else {
		if st, err := os.Stat(cfg.BasePath); err != nil {
			w = append(w, "base_path does not exist: "+cfg.BasePath)
		} else if !st.IsDir() {
			w = append(w, "base_path is not a directory: "+cfg.BasePath)
		}
	}
	if cfg.MaxChunk > cfg.MaxPayload {
		w = append(w, "max_chunk is larger than max_payload (will cause TOO_LARGE)")
	}
	if cfg.Endpoint == "" || cfg.Endpoint[0] != '/' {
		w = append(w, "endpoint should start with '/' (e.g. /wicos64/api)")
	}
	if !cfg.EnableAdminUI {
		w = append(w, "admin UI is disabled")
	}
	if cfg.AdminAllowRemote && cfg.AdminPassword == "" {
		w = append(w, "admin_allow_remote=true without admin_password – consider enabling BasicAuth")
	}
	if cfg.TmpCleanupEnabled {
		if cfg.TmpCleanupIntervalSec <= 0 {
			w = append(w, "tmp_cleanup_interval_sec is <= 0 (cleanup will fallback to 900s)")
		}
		if cfg.TmpCleanupMaxAgeSec <= 0 {
			w = append(w, "tmp_cleanup_max_age_sec is <= 0 (cleanup will fallback to 24h)")
		}
	}
	if cfg.TrashEnabled {
		td := strings.TrimSpace(cfg.TrashDir)
		if td == "" {
			td = ".TRASH"
		}
		w = append(w, fmt.Sprintf("trash enabled: RM/RMDIR and overwrite keep previous data under %q (still counts towards quota until deleted)", td))
		if strings.EqualFold(td, ".TMP") {
			w = append(w, "trash_dir is set to .TMP – this will collide with temp files")
		}
	}
	if cfg.TrashCleanupEnabled && !cfg.TrashEnabled {
		w = append(w, "trash_cleanup_enabled=true but trash_enabled=false (cleanup will do nothing)")
	}

	if cfg.Bootstrap.Enabled {
		if strings.TrimSpace(cfg.Bootstrap.Token) == "" {
			w = append(w, "bootstrap.enabled=true but bootstrap.token is empty")
		}
		if !cfg.Bootstrap.AllowGET && !cfg.Bootstrap.AllowPOST {
			w = append(w, "bootstrap.enabled=true but allow_get=false and allow_post=false (endpoint will be unreachable)")
		}
		if !cfg.Bootstrap.LanOnly {
			w = append(w, "bootstrap.lan_only=false – NOT RECOMMENDED (would expose tokens beyond LAN)")
		}
		if len(cfg.Bootstrap.MacTokens) == 0 {
			w = append(w, "bootstrap enabled but no bootstrap.mac_tokens configured")
		}
	}
	return w
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	f := parseLogFilter(r)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(s.logs.filteredSnapshot(f))
}

func (s *Server) handleAdminLogsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.logs.clear()
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK\n"))
}

func (s *Server) handleAdminLogsExport(w http.ResponseWriter, r *http.Request) {
	f := parseLogFilter(r)
	format := r.URL.Query().Get("format")
	if format == "" {
		format = "jsonl"
	}
	entries := s.logs.filteredSnapshot(f)
	if format == "text" || format == "txt" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Disposition", "attachment; filename=logs.txt")
		w.WriteHeader(http.StatusOK)
		for _, e := range entries {
			_, _ = w.Write([]byte(formatLogLine(e)))
			_, _ = w.Write([]byte("\n"))
		}
		return
	}
	// default: jsonl
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", "attachment; filename=logs.jsonl")
	w.WriteHeader(http.StatusOK)
	for _, e := range entries {
		_, _ = w.Write(e.jsonLine())
		_, _ = w.Write([]byte("\n"))
	}
}

func (s *Server) handleAdminLogStream(w http.ResponseWriter, r *http.Request) {
	// Server-sent events stream.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	fl, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	ch, cancel := s.logs.subscribe()
	defer cancel()

	// Initial comment (keeps some proxies happy).
	_, _ = w.Write([]byte(": ok\n\n"))
	fl.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			// One event per entry.
			// NOTE: Use JSON per line to keep it simple.
			_, _ = w.Write([]byte("data: "))
			_, _ = w.Write(e.jsonLine())
			_, _ = w.Write([]byte("\n\n"))
			fl.Flush()
		}
	}
}

func clientIP(r *http.Request) string {
	// We deliberately do not trust X-Forwarded-For here.
	// If you want admin access behind a reverse proxy, set AdminAllowRemote=true
	// and protect with AdminPassword.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *Server) handleAdminChartJS(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(chartJS)
}

func (s *Server) handleAdminLogoSVG(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(logoSVG)
}
