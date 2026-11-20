"""HTTP API + static file server (stdlib only)."""
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .db import connect, dumps, loads, reset_db
from .pipeline import create_correction
from .seed import seed

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static")


def _rows(conn, sql, args=()):
    return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _row(conn, sql, args=()):
    r = conn.execute(sql, args).fetchone()
    return dict(r) if r else None


class Api:
    """Route handlers; each returns (status, obj)."""

    def __init__(self, db_path, engine):
        self.db_path = db_path
        self.engine = engine

    # ---------------- overview / lists ----------------
    def overview(self):
        conn = connect(self.db_path)
        try:
            q = lambda sql: conn.execute(sql).fetchone()[0]
            return 200, {
                "sources": q("SELECT COUNT(*) FROM sources"),
                "batches": q("SELECT COUNT(*) FROM batches"),
                "records": q("SELECT COUNT(*) FROM parse_records"),
                "entities": q("SELECT COUNT(*) FROM entities WHERE state='active'"),
                "corrections_open": q("SELECT COUNT(*) FROM corrections WHERE status IN ('pending','running')"),
                "corrections_conflicted": q("SELECT COUNT(*) FROM corrections WHERE status='conflicted'"),
                "tasks_queued": q("SELECT COUNT(*) FROM tasks WHERE status='queued'"),
                "tasks_running": q("SELECT COUNT(*) FROM tasks WHERE status='running'"),
            }
        finally:
            conn.close()

    def sources(self):
        conn = connect(self.db_path)
        try:
            return 200, _rows(conn, "SELECT * FROM sources ORDER BY id")
        finally:
            conn.close()

    def batches(self, query):
        conn = connect(self.db_path)
        try:
            where, args = "", []
            if query.get("source_id"):
                where, args = "WHERE b.source_id=?", [int(query["source_id"][0])]
            rows = _rows(conn, f"""
                SELECT b.*, s.name AS source_name,
                  (SELECT COUNT(*) FROM corrections c WHERE c.batch_id=b.id
                     AND c.status IN ('pending','running')) AS open_corrections,
                  (SELECT COUNT(*) FROM corrections c WHERE c.batch_id=b.id
                     AND c.status='conflicted') AS conflicted_corrections
                FROM batches b JOIN sources s ON s.id=b.source_id {where}
                ORDER BY b.id DESC""", args)
            return 200, rows
        finally:
            conn.close()

    def batch_detail(self, batch_id):
        conn = connect(self.db_path)
        try:
            b = _row(conn, """
                SELECT b.*, s.name AS source_name, s.kind AS source_kind
                FROM batches b JOIN sources s ON s.id=b.source_id WHERE b.id=?""",
                     (batch_id,))
            if not b:
                return 404, {"error": "批次不存在"}
            b["versions"] = _rows(conn, """
                SELECT bv.*,
                  (SELECT COUNT(*) FROM raw_files f WHERE f.batch_version_id=bv.id) AS files,
                  (SELECT COUNT(*) FROM parse_records r WHERE r.batch_version_id=bv.id) AS records,
                  (SELECT COUNT(*) FROM parse_records r WHERE r.batch_version_id=bv.id
                     AND r.status='error') AS record_errors,
                  (SELECT COUNT(*) FROM entities e WHERE e.batch_version_id=bv.id) AS entities
                FROM batch_versions bv WHERE bv.batch_id=? ORDER BY bv.version""",
                                  (batch_id,))
            b["corrections"] = _rows(conn,
                "SELECT * FROM corrections WHERE batch_id=? ORDER BY id DESC", (batch_id,))
            return 200, b
        finally:
            conn.close()

    # ---------------- lineage / graph ----------------
    def lineage(self, batch_id):
        """Full trace: source -> batch -> versions -> files -> records -> entities."""
        conn = connect(self.db_path)
        try:
            b = _row(conn, "SELECT * FROM batches WHERE id=?", (batch_id,))
            if not b:
                return 404, {"error": "批次不存在"}
            src = _row(conn, "SELECT * FROM sources WHERE id=?", (b["source_id"],))
            versions = _rows(conn,
                "SELECT * FROM batch_versions WHERE batch_id=? ORDER BY version", (batch_id,))
            for v in versions:
                v["files"] = _rows(conn,
                    "SELECT id, batch_version_id, filename, checksum, size_bytes"
                    " FROM raw_files WHERE batch_version_id=?", (v["id"],))
                v["records"] = _rows(conn, """
                    SELECT id, batch_version_id, raw_file_id, record_key, status, error_msg
                    FROM parse_records WHERE batch_version_id=? ORDER BY id""", (v["id"],))
                v["entities"] = _rows(conn, """
                    SELECT id, batch_version_id, parse_record_id, entity_type, entity_key, state
                    FROM entities WHERE batch_version_id=? ORDER BY id""", (v["id"],))
                v["tasks"] = _rows(conn, """
                    SELECT id, task_type, status, depends_on, supersedes, result, attempt
                    FROM tasks WHERE batch_version_id=? ORDER BY id""", (v["id"],))
            return 200, {"source": src, "batch": b, "versions": versions}
        finally:
            conn.close()

    def record_detail(self, record_id):
        conn = connect(self.db_path)
        try:
            r = _row(conn, "SELECT * FROM parse_records WHERE id=?", (record_id,))
            if not r:
                return 404, {"error": "记录不存在"}
            r["payload"] = loads(r["payload"])
            r["entities"] = _rows(conn,
                "SELECT * FROM entities WHERE parse_record_id=?", (record_id,))
            f = _row(conn, "SELECT * FROM raw_files WHERE id=?", (r["raw_file_id"],))
            v = _row(conn, "SELECT * FROM batch_versions WHERE id=?",
                     (r["batch_version_id"],))
            b = _row(conn, "SELECT * FROM batches WHERE id=?", (v["batch_id"],))
            s = _row(conn, "SELECT * FROM sources WHERE id=?", (b["source_id"],))
            r["provenance"] = {"file": f, "version": v, "batch": b, "source": s}
            return 200, r
        finally:
            conn.close()

    def entity_detail(self, entity_id):
        conn = connect(self.db_path)
        try:
            e = _row(conn, "SELECT * FROM entities WHERE id=?", (entity_id,))
            if not e:
                return 404, {"error": "实体不存在"}
            e["payload"] = loads(e["payload"])
            if e["parse_record_id"]:
                r = _row(conn, "SELECT * FROM parse_records WHERE id=?",
                         (e["parse_record_id"],))
                if r:
                    r["payload"] = loads(r["payload"])
                e["parse_record"] = r
            v = _row(conn, "SELECT * FROM batch_versions WHERE id=?",
                     (e["batch_version_id"],))
            b = _row(conn, "SELECT * FROM batches WHERE id=?", (v["batch_id"],))
            s = _row(conn, "SELECT * FROM sources WHERE id=?", (b["source_id"],))
            e["provenance"] = {"version": v, "batch": b, "source": s}
            return 200, e
        finally:
            conn.close()

    # ---------------- corrections ----------------
    def corrections(self, query):
        conn = connect(self.db_path)
        try:
            where, args = "", []
            if query.get("batch_id"):
                where, args = "WHERE c.batch_id=?", [int(query["batch_id"][0])]
            rows = _rows(conn, f"""
                SELECT c.*, b.label AS batch_label
                FROM corrections c JOIN batches b ON b.id=c.batch_id
                {where} ORDER BY c.id DESC""", args)
            for r in rows:
                r["impact"] = loads(r["impact"])
            return 200, rows
        finally:
            conn.close()

    def correction_detail(self, cid):
        conn = connect(self.db_path)
        try:
            c = _row(conn, """
                SELECT c.*, b.label AS batch_label FROM corrections c
                JOIN batches b ON b.id=c.batch_id WHERE c.id=?""", (cid,))
            if not c:
                return 404, {"error": "纠错不存在"}
            c["impact"] = loads(c["impact"])
            c["items"] = _rows(conn,
                "SELECT * FROM correction_items WHERE correction_id=?", (cid,))
            for it in c["items"]:
                it["patch"] = loads(it["patch"])
            # re-run results: the replacement task chain produced by this correction
            if c["impact"].get("requeued_tasks"):
                ids = c["impact"]["requeued_tasks"]
                ph = ",".join("?" * len(ids))
                c["rerun_tasks"] = _rows(conn,
                    f"SELECT id, task_type, status, result, started_at, finished_at"
                    f" FROM tasks WHERE id IN ({ph}) ORDER BY id", ids)
                for t in c["rerun_tasks"]:
                    t["result"] = loads(t["result"])
            else:
                c["rerun_tasks"] = []
            c["task"] = _row(conn,
                "SELECT id, status, result FROM tasks WHERE correction_id=?"
                " AND task_type='correction' ORDER BY id DESC LIMIT 1", (cid,))
            if c["task"]:
                c["task"]["result"] = loads(c["task"]["result"])
            return 200, c
        finally:
            conn.close()

    def create_correction(self, batch_id, body):
        items = body.get("items") or []
        base_version = body.get("base_version")
        conn = connect(self.db_path)
        try:
            b = _row(conn, "SELECT current_version FROM batches WHERE id=?", (batch_id,))
            if not b:
                return 404, {"error": "批次不存在"}
            if base_version is None:
                base_version = b["current_version"]
        finally:
            conn.close()
        cid, err = create_correction(
            self.db_path, batch_id, int(base_version),
            body.get("reason", ""), items, body.get("created_by", "auditor"))
        if err:
            return 400, {"error": err}
        task_id = self.engine.enqueue_correction(cid)
        return 201, {"correction_id": cid, "task_id": task_id}

    def cancel_correction(self, cid):
        ok, msg = self.engine.cancel_correction(int(cid))
        return (200 if ok else 409), {"ok": ok, "message": msg}

    # ---------------- tasks / events ----------------
    def tasks(self, query):
        conn = connect(self.db_path)
        try:
            where, args = "", []
            if query.get("batch_version_id"):
                where = "WHERE t.batch_version_id=?"
                args = [int(query["batch_version_id"][0])]
            elif query.get("batch_id"):
                where = ("WHERE t.batch_version_id IN "
                         "(SELECT id FROM batch_versions WHERE batch_id=?)")
                args = [int(query["batch_id"][0])]
            rows = _rows(conn, f"""
                SELECT t.*, bv.version, bv.batch_id
                FROM tasks t LEFT JOIN batch_versions bv ON bv.id=t.batch_version_id
                {where} ORDER BY t.id DESC LIMIT 200""", args)
            for r in rows:
                r["depends_on"] = loads(r["depends_on"], [])
                r["result"] = loads(r["result"])
            return 200, rows
        finally:
            conn.close()

    def cancel_task(self, task_id):
        ok, msg = self.engine.cancel_task(int(task_id))
        return (200 if ok else 409), {"ok": ok, "message": msg}

    def events(self, query):
        limit = int((query.get("limit") or [80])[0])
        conn = connect(self.db_path)
        try:
            return 200, _rows(conn,
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
        finally:
            conn.close()

    def reseed(self):
        from .pipeline import Engine
        self.engine.stop()
        reset_db(self.db_path)
        self.engine = Engine(self.db_path)
        self.engine.start()
        Handler.api = self  # engine swapped; keep handler reference fresh
        info = seed(self.db_path, self.engine)
        return 200, {"ok": True, **info}


# ---------------------------------------------------------------------------
class Handler(BaseHTTPRequestHandler):
    api: Api = None  # injected by serve()
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # quiet
        pass

    # -- helpers
    def _send(self, status, obj, ctype="application/json; charset=utf-8"):
        body = obj if isinstance(obj, bytes) else dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _query(self):
        from urllib.parse import urlparse, parse_qs
        u = urlparse(self.path)
        return u.path, parse_qs(u.query)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode() or "{}")
        except json.JSONDecodeError:
            return {}

    # -- routing
    def do_GET(self):
        path, q = self._query()
        a = self.api
        routes = [
            (r"^/api/overview$", lambda m: a.overview()),
            (r"^/api/sources$", lambda m: a.sources()),
            (r"^/api/batches$", lambda m: a.batches(q)),
            (r"^/api/batches/(\d+)$", lambda m: a.batch_detail(int(m.group(1)))),
            (r"^/api/batches/(\d+)/lineage$", lambda m: a.lineage(int(m.group(1)))),
            (r"^/api/records/(\d+)$", lambda m: a.record_detail(int(m.group(1)))),
            (r"^/api/entities/(\d+)$", lambda m: a.entity_detail(int(m.group(1)))),
            (r"^/api/corrections$", lambda m: a.corrections(q)),
            (r"^/api/corrections/(\d+)$", lambda m: a.correction_detail(int(m.group(1)))),
            (r"^/api/tasks$", lambda m: a.tasks(q)),
            (r"^/api/events$", lambda m: a.events(q)),
        ]
        for pat, fn in routes:
            m = re.match(pat, path)
            if m:
                status, obj = fn(m)
                return self._send(status, obj)
        if path == "/" or path.startswith("/static/"):
            return self._static(path)
        self._send(404, {"error": f"not found: {path}"})

    def do_POST(self):
        path, q = self._query()
        a = self.api
        body = self._body()
        routes = [
            (r"^/api/batches/(\d+)/corrections$",
             lambda m: a.create_correction(int(m.group(1)), body)),
            (r"^/api/corrections/(\d+)/cancel$",
             lambda m: a.cancel_correction(int(m.group(1)))),
            (r"^/api/tasks/(\d+)/cancel$", lambda m: a.cancel_task(int(m.group(1)))),
            (r"^/api/seed$", lambda m: a.reseed()),
        ]
        for pat, fn in routes:
            m = re.match(pat, path)
            if m:
                status, obj = fn(m)
                return self._send(status, obj)
        self._send(404, {"error": f"not found: {path}"})

    def _static(self, path):
        name = "index.html" if path == "/" else path[len("/static/"):]
        name = os.path.normpath(name).lstrip("/")
        full = os.path.join(STATIC_DIR, name)
        if not full.startswith(STATIC_DIR) or not os.path.isfile(full):
            return self._send(404, {"error": "not found"})
        ctype = {".html": "text/html; charset=utf-8", ".js": "text/javascript",
                 ".css": "text/css"}.get(os.path.splitext(full)[1], "application/octet-stream")
        with open(full, "rb") as f:
            self._send(200, f.read(), ctype)


def make_server(db_path, engine, port=8000):
    Handler.api = Api(db_path, engine)
    return ThreadingHTTPServer(("127.0.0.1", port), Handler)
