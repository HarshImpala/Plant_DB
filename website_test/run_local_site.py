"""
Local-only static server for the generated site.

This does not affect GitHub Pages deployment.
It only serves files from ./output on your machine.
"""

from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import argparse
import os
import shutil


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def main():
    parser = argparse.ArgumentParser(description="Serve the generated site locally.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    if not OUTPUT_DIR.exists():
        raise SystemExit(f"Missing output directory: {OUTPUT_DIR}\nRun generator/build_site.py first.")

    # Ensure core static assets exist in output so styles load.
    output_static = OUTPUT_DIR / "static"
    output_css = output_static / "css"
    expected_css = output_css / "style-base.css"
    if not expected_css.exists():
        source_static = BASE_DIR / "static"
        if source_static.exists():
            output_static.mkdir(parents=True, exist_ok=True)
            shutil.copytree(source_static, output_static, dirs_exist_ok=True)
            print("Synced static assets into output/ (missing CSS detected).")
        else:
            print("Warning: static assets missing; styling may be broken.")

    os.chdir(OUTPUT_DIR)
    server = ThreadingHTTPServer((args.host, args.port), SimpleHTTPRequestHandler)
    print(f"Serving {OUTPUT_DIR} at http://{args.host}:{args.port}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Server stopped.")


if __name__ == "__main__":
    main()
