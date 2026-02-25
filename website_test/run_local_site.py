"""
Local-only static server for the generated site.

This does not affect GitHub Pages deployment.
It only serves files from ./output on your machine.
"""

from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import argparse
import os


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def main():
    parser = argparse.ArgumentParser(description="Serve the generated site locally.")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind (default: 8000)")
    args = parser.parse_args()

    if not OUTPUT_DIR.exists():
        raise SystemExit(f"Missing output directory: {OUTPUT_DIR}\nRun generator/build_site.py first.")

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
