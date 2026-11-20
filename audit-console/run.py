#!/usr/bin/env python3
"""Launch the collection audit console.

Usage: python3 run.py [--port 8000] [--db audit.db] [--seed]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.db import init_db, reset_db  # noqa: E402
from app.pipeline import Engine  # noqa: E402
from app.seed import seed  # noqa: E402
from app.server import make_server  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "audit.db"))
    ap.add_argument("--reseed", action="store_true", help="drop and reload demo data")
    args = ap.parse_args()

    if args.reseed or not os.path.exists(args.db):
        reset_db(args.db)
    else:
        init_db(args.db)

    engine = Engine(args.db)
    engine.start()
    from app.db import connect
    conn = connect(args.db)
    has_data = conn.execute("SELECT COUNT(*) FROM sources").fetchone()[0] > 0
    conn.close()
    if not has_data:
        seed(args.db, engine)

    httpd = make_server(args.db, engine, args.port)
    print(f"采集审计台 → http://127.0.0.1:{args.port}  (db: {args.db})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        engine.stop()
        httpd.server_close()


if __name__ == "__main__":
    main()
