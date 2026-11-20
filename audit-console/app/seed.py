"""Demo dataset: 2 sources, 4 batches, raw files with good + broken records."""
import hashlib

from .db import connect, dumps, log_event, now


def _file_payload(records):
    return dumps({"records": records})


def _checksum(text):
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def seed(db_path, engine=None):
    """Insert demo rows and enqueue ingest chains. Idempotent per reset_db."""
    conn = connect(db_path)
    try:
        s1 = conn.execute(
            "INSERT INTO sources(name, kind, created_at) VALUES(?,?,?)",
            ("供应商FTP-华东", "ftp", now())).lastrowid
        s2 = conn.execute(
            "INSERT INTO sources(name, kind, created_at) VALUES(?,?,?)",
            ("开放平台API", "api", now())).lastrowid

        def batch(source_id, label):
            bid = conn.execute(
                "INSERT INTO batches(source_id, label, status, created_at)"
                " VALUES(?,?,'received',?)", (source_id, label, now())).lastrowid
            bv = conn.execute(
                "INSERT INTO batch_versions(batch_id, version, created_at)"
                " VALUES(?,1,?)", (bid, now())).lastrowid
            return bid, bv

        def add_file(bv, filename, records):
            payload = _file_payload(records)
            conn.execute(
                """INSERT INTO raw_files(batch_version_id, filename, checksum,
                                         size_bytes, payload, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (bv, filename, _checksum(payload), len(payload), payload, now()))

        def order(key, **kw):
            d = {"type": "order", "amount": 100, "currency": "CNY"}
            d.update(kw)
            return {"key": key, "status": "ok", "data": d}

        # batch 1: clean
        b1, bv1 = batch(s1, "B-20261118-01")
        add_file(bv1, "orders_1118_a.csv", [
            order("PO-1001", amount=320), order("PO-1002", amount=88),
            {"key": "CUST-01", "status": "ok",
             "data": {"type": "customer", "name": "杭州云杉"}},
        ])
        # batch 2: contains broken records -> the correction demo target
        b2, bv2 = batch(s1, "B-20261119-01")
        add_file(bv2, "orders_1119_a.csv", [
            order("PO-2001", amount=150),
            {"key": "PO-2002", "status": "error", "data": {"type": "order"},
             "error": "amount 字段缺失"},
            {"key": "PO-2003", "status": "error", "data": {"type": "order", "amount": -5},
             "error": "金额为负"},
            order("PO-2004", amount=60),
        ])
        add_file(bv2, "orders_1119_b.csv", [
            order("PO-2005", amount=210),
            {"key": "SKU-09", "status": "warn",
             "data": {"type": "sku", "name": "螺栓M8", "uom": ""}},
        ])
        # batch 3: second source
        b3, bv3 = batch(s2, "API-20261119-01")
        add_file(bv3, "push_1119.json", [
            {"key": "CUST-88", "status": "ok",
             "data": {"type": "customer", "name": "苏州集合"}},
            order("PO-3001", amount=999),
        ])
        # batch 4: small, fast
        b4, bv4 = batch(s1, "B-20261120-01")
        add_file(bv4, "orders_1120.csv", [order("PO-4001", amount=45)])

        log_event(conn, "system", "演示数据已装载: 2 来源 / 4 批次")
        conn.commit()
        chains = []
        if engine:
            for bv in (bv1, bv2, bv3, bv4):
                chains.append(engine.enqueue_ingest_chain(bv))
        return {"sources": [s1, s2], "batches": [b1, b2, b3, b4],
                "versions": [bv1, bv2, bv3, bv4], "tasks": chains}
    finally:
        conn.close()
