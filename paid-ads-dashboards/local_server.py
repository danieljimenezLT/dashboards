"""
local_server.py  —  Dashboard local server with run-analysis endpoint.

Replaces  python -m http.server 8080
Use with: python local_server.py

Serves all static files from the repo root AND handles:
  POST /api/run-analysis
    1. Downloads the latest daniel-paid-ads-data.json from GitHub
    2. Runs analyze_ads.py to rebuild paid-ads-analysis.json
    3. Returns JSON { status, message }
"""
import http.server, socketserver, json, subprocess, pathlib, sys
import urllib.request, urllib.error

PORT  = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
ROOT  = pathlib.Path(__file__).parent          # paid-ads-dashboards/
SERVE = ROOT.parent                             # repo root (serves all dashboards)

DATA_GITHUB_URL = (
    "https://raw.githubusercontent.com/dev-leadteam/dashboards/main"
    "/paid-ads-dashboards/daniel-paid-ads-data.json"
)


class Handler(http.server.SimpleHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(SERVE), **kwargs)

    # ── POST /api/run-analysis ────────────────────────────────────────────────
    def do_POST(self):
        if self.path == "/api/run-analysis":
            self._handle_run_analysis()
        else:
            self.send_error(404, "Not found")

    def do_OPTIONS(self):
        self._cors_headers(200)
        self.end_headers()

    def _handle_run_analysis(self):
        steps = []
        try:
            # Step 1 — download latest data from GitHub
            steps.append("Downloading latest data from GitHub...")
            print(f"\n[run-analysis] {steps[-1]}")
            try:
                with urllib.request.urlopen(DATA_GITHUB_URL, timeout=30) as resp:
                    raw = resp.read()
                dest = ROOT / "daniel-paid-ads-data.json"
                dest.write_bytes(raw)
                steps.append(f"Downloaded {len(raw):,} bytes -> daniel-paid-ads-data.json")
                print(f"[run-analysis] {steps[-1]}")
            except urllib.error.URLError as e:
                steps.append(f"GitHub download failed ({e}) — using existing local data")
                print(f"[run-analysis] WARNING: {steps[-1]}")

            # Step 2 — run analyze_ads.py
            steps.append("Running analyze_ads.py...")
            print(f"[run-analysis] {steps[-1]}")
            result = subprocess.run(
                [sys.executable, str(ROOT / "analyze_ads.py")],
                capture_output=True, text=True, cwd=str(ROOT), timeout=120,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr or "analyze_ads.py exited with error")
            for line in result.stdout.strip().splitlines():
                steps.append(line)
                print(f"[run-analysis] {line}")

            self._json_response(200, {"status": "ok", "steps": steps})

        except Exception as exc:
            msg = str(exc)
            steps.append(f"ERROR: {msg}")
            print(f"[run-analysis] ERROR: {msg}")
            self._json_response(500, {"status": "error", "message": msg, "steps": steps})

    # ── helpers ───────────────────────────────────────────────────────────────
    def _cors_headers(self, code):
        self.send_response(code)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json_response(self, code, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self._cors_headers(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        # Silence noisy static-file GETs; keep API calls visible
        if "/api/" in (args[0] if args else ""):
            super().log_message(fmt, *args)


# ── Start ─────────────────────────────────────────────────────────────────────
print(f"Dashboard server running at http://localhost:{PORT}")
print(f"  -> http://localhost:{PORT}/paid-ads-dashboards/paid-ads.html")
print(f"  -> POST http://localhost:{PORT}/api/run-analysis")
socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
