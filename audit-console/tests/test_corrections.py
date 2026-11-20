"""Verification: concurrent corrections, cancellation, lineage, immutability."""
import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db import connect, loads, reset_db  # noqa: E402
from app.pipeline import Engine, apply_correction, create_correction  # noqa: E402
from app.seed import seed  # noqa: E402


def wait_for(cond, timeout=10, interval=0.05, what="condition"):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if cond():
            return True
        time.sleep(interval)
    raise AssertionError(f"timeout waiting for {what}")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "t.db")
        reset_db(self.db)
        self.engine = Engine(self.db, workers=2, step_delay=0.05)
        self.engine.start()
        self.info = seed(self.db, self.engine)
        # batch 2 (B-20261119-01) holds the broken records
        self.batch2 = self.info["batches"][1]
        wait_for(lambda: self._tasks_done(), what="ingest chain to finish")

    def tearDown(self):
        self.engine.stop()
        self.tmp.cleanup()

    def _tasks_done(self):
        conn = connect(self.db)
        try:
            n = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status IN ('queued','running')"
            ).fetchone()[0]
            return n == 0
        finally:
            conn.close()

    def q(self, sql, args=()):
        conn = connect(self.db)
        try:
            return conn.execute(sql, args).fetchall()
        finally:
            conn.close()

    def q1(self, sql, args=()):
        return self.q(sql, args)[0]


class TestLineage(Base):
    def test_full_chain_traceable(self):
        """批次→来源→文件→记录→实体 every hop joins up."""
        row = self.q1("""
            SELECT s.name AS source, b.label, bv.version, f.filename,
                   r.record_key, e.entity_key
            FROM entities e
            JOIN parse_records r ON r.id=e.parse_record_id
            JOIN raw_files f ON f.id=r.raw_file_id
            JOIN batch_versions bv ON bv.id=r.batch_version_id
            JOIN batches b ON b.id=bv.batch_id
            JOIN sources s ON s.id=b.source_id
            WHERE b.id=? AND e.entity_type!='source_rollup' LIMIT 1""",
                      (self.batch2,))
        self.assertEqual(row["source"], "供应商FTP-华东")
        self.assertEqual(row["label"], "B-20261119-01")
        self.assertTrue(row["filename"].endswith(".csv"))
        self.assertTrue(row["entity_key"].split(":", 1)[1], row["record_key"])

    def test_error_records_parsed_with_message(self):
        rows = self.q("""
            SELECT r.record_key, r.error_msg FROM parse_records r
            JOIN batch_versions bv ON bv.id=r.batch_version_id
            WHERE bv.batch_id=? AND r.status='error'""", (self.batch2,))
        keys = {r["record_key"] for r in rows}
        self.assertEqual(keys, {"PO-2002", "PO-2003"})
        self.assertTrue(all(r["error_msg"] for r in rows))

    def test_rollup_entity_written(self):
        n = self.q1("""
            SELECT COUNT(*) c FROM entities e
            JOIN batch_versions bv ON bv.id=e.batch_version_id
            WHERE bv.batch_id=? AND e.entity_type='source_rollup'""",
                    (self.batch2,))["c"]
        self.assertEqual(n, 1)


class TestCorrection(Base):
    def _fix_items(self):
        return [
            {"action": "fix", "record_key": "PO-2002",
             "patch": {"payload": {"type": "order", "amount": 150}, "status": "ok"}},
            {"action": "drop", "record_key": "PO-2003"},
            {"action": "add", "record_key": "PO-2006",
             "patch": {"payload": {"type": "order", "amount": 77}, "status": "ok"}},
        ]

    def test_correction_new_version_keeps_original(self):
        cid, err = create_correction(self.db, self.batch2, 1, "供应商补发", self._fix_items())
        self.assertIsNone(err)
        self.engine.enqueue_correction(cid)
        wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                 (cid,))["status"] == "done", what="correction done")
        # version advanced, old version superseded but intact
        b = self.q1("SELECT current_version FROM batches WHERE id=?", (self.batch2,))
        self.assertEqual(b["current_version"], 2)
        v1 = self.q1("""SELECT state FROM batch_versions
                        WHERE batch_id=? AND version=1""", (self.batch2,))
        self.assertEqual(v1["state"], "superseded")
        # 原结论保留: v1 error records still there with original messages
        errs = self.q("""SELECT r.record_key, r.error_msg FROM parse_records r
                         JOIN batch_versions bv ON bv.id=r.batch_version_id
                         WHERE bv.batch_id=? AND bv.version=1 AND r.status='error'""",
                      (self.batch2,))
        self.assertEqual({r["record_key"] for r in errs}, {"PO-2002", "PO-2003"})
        # v1 entities kept, marked superseded
        n_old = self.q1("""SELECT COUNT(*) c FROM entities e
                           JOIN batch_versions bv ON bv.id=e.batch_version_id
                           WHERE bv.batch_id=? AND bv.version=1 AND e.state='superseded'""",
                        (self.batch2,))["c"]
        self.assertGreater(n_old, 0)
        # v2 records: fixed, dropped, added
        v2_keys = {r["record_key"]: r["status"] for r in self.q(
            """SELECT r.record_key, r.status FROM parse_records r
               JOIN batch_versions bv ON bv.id=r.batch_version_id
               WHERE bv.batch_id=? AND bv.version=2""", (self.batch2,))}
        self.assertEqual(v2_keys.get("PO-2002"), "ok")
        self.assertNotIn("PO-2003", v2_keys)
        self.assertEqual(v2_keys.get("PO-2006"), "ok")
        fixed = self.q1("""SELECT r.payload FROM parse_records r
                           JOIN batch_versions bv ON bv.id=r.batch_version_id
                           WHERE bv.batch_id=? AND bv.version=2 AND r.record_key='PO-2002'""",
                        (self.batch2,))
        self.assertEqual(loads(fixed["payload"])["amount"], 150)

    def test_downstream_requeued_by_impact(self):
        cid, _ = create_correction(self.db, self.batch2, 1, "fix", self._fix_items())
        self.engine.enqueue_correction(cid)
        wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                 (cid,))["status"] == "done", what="correction done")
        wait_for(self._tasks_done, what="requeued chain")
        impact = loads(self.q1("SELECT impact FROM corrections WHERE id=?",
                               (cid,))["impact"])
        self.assertEqual(set(impact["affected_record_keys"]),
                         {"PO-2002", "PO-2003", "PO-2006"})
        self.assertEqual(len(impact["requeued_tasks"]), 2)  # extract + aggregate
        # requeued chain actually ran on v2 and rebuilt entities
        v2_entities = self.q1("""SELECT COUNT(*) c FROM entities e
                                 JOIN batch_versions bv ON bv.id=e.batch_version_id
                                 WHERE bv.batch_id=? AND bv.version=2
                                   AND e.state='active' AND e.entity_type='order'""",
                              (self.batch2,))["c"]
        self.assertEqual(v2_entities, 5)  # 2001,2002,2004,2005,2006
        done = self.q("""SELECT task_type, status FROM tasks
                         WHERE id IN (?,?)""", impact["requeued_tasks"])
        self.assertTrue(all(t["status"] == "done" for t in done))

    def test_concurrent_corrections_exactly_one_wins(self):
        """Two corrections on the same base version, applied simultaneously:
        exactly one commits v2; the other exits `conflicted` with a reason."""
        ids = []
        for who in ("alice", "bob"):
            cid, err = create_correction(
                self.db, self.batch2, 1, f"{who} 的修正",
                [{"action": "fix", "record_key": "PO-2002",
                  "patch": {"payload": {"amount": 999}, "status": "ok"}}],
                created_by=who)
            self.assertIsNone(err)
            ids.append(cid)

        barrier = threading.Barrier(2)
        results = {}

        def apply(cid):
            conn = connect(self.db)
            try:
                barrier.wait()
                results[cid] = apply_correction(conn, cid, engine=self.engine)
            finally:
                conn.close()

        threads = [threading.Thread(target=apply, args=(c,)) for c in ids]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        statuses = {c: self.q1("SELECT status, conflict_reason FROM corrections"
                               " WHERE id=?", (c,)) for c in ids}
        done = [c for c, s in statuses.items() if s["status"] == "done"]
        conflicted = [c for c, s in statuses.items() if s["status"] == "conflicted"]
        self.assertEqual(len(done), 1, f"expected 1 winner, got {statuses}")
        self.assertEqual(len(conflicted), 1)
        self.assertIn("版本冲突", statuses[conflicted[0]]["conflict_reason"])
        # version advanced exactly once — no lost update, no double apply
        b = self.q1("SELECT current_version FROM batches WHERE id=?", (self.batch2,))
        self.assertEqual(b["current_version"], 2)
        n_versions = self.q1("SELECT COUNT(*) c FROM batch_versions WHERE batch_id=?",
                             (self.batch2,))["c"]
        self.assertEqual(n_versions, 2)

    def test_stale_base_version_conflicts(self):
        cid1, _ = create_correction(self.db, self.batch2, 1, "first",
                                    [{"action": "drop", "record_key": "PO-2003"}])
        self.engine.enqueue_correction(cid1)
        wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                 (cid1,))["status"] == "done", what="first correction")
        # authored against v1, but batch is already at v2
        cid2, _ = create_correction(self.db, self.batch2, 1, "stale",
                                    [{"action": "drop", "record_key": "PO-2004"}])
        self.engine.enqueue_correction(cid2)
        wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                 (cid2,))["status"] == "conflicted",
                 what="stale correction to conflict")
        reason = self.q1("SELECT conflict_reason FROM corrections WHERE id=?",
                         (cid2,))["conflict_reason"]
        self.assertIn("v1", reason)
        self.assertIn("v2", reason)


class TestCancellation(Base):
    def test_cancel_queued_task_cascades_to_dependents(self):
        # stop workers so nothing gets claimed
        self.engine.stop()
        conn = connect(self.db)
        bv = conn.execute("SELECT id FROM batch_versions WHERE batch_id=? AND version=1",
                          (self.batch2,)).fetchone()["id"]
        conn.close()
        p = self.engine.enqueue("parse", bv)
        x = self.engine.enqueue("extract", bv, depends_on=[p])
        ok, _ = self.engine.cancel_task(p)
        self.assertTrue(ok)
        st = {r["id"]: r["status"] for r in self.q(
            "SELECT id, status FROM tasks WHERE id IN (?,?)", (p, x))}
        self.assertEqual(st[p], "cancelled")
        self.assertEqual(st[x], "cancelled")  # dependent cascaded

    def test_cancel_running_task(self):
        self.engine.stop()  # only the slow engine may claim tasks from here
        slow = Engine(self.db, workers=1, step_delay=0.4)
        slow.start()
        try:
            conn = connect(self.db)
            bv = conn.execute("SELECT id FROM batch_versions WHERE batch_id=?"
                              " AND version=1", (self.batch2,)).fetchone()["id"]
            conn.close()
            tid = slow.enqueue("parse", bv)
            wait_for(lambda: self.q1("SELECT status FROM tasks WHERE id=?",
                                     (tid,))["status"] == "running", what="task running")
            ok, msg = slow.cancel_task(tid)
            self.assertTrue(ok)
            wait_for(lambda: self.q1("SELECT status FROM tasks WHERE id=?",
                                     (tid,))["status"] == "cancelled",
                     what="task cancelled")
            # partial work rolled back: no new parse rows from this run
            n = self.q1("""SELECT COUNT(*) c FROM parse_records
                           WHERE batch_version_id=?""", (bv,))["c"]
            self.assertEqual(n, 6)  # exactly the original ingest output
        finally:
            slow.stop()

    def test_cancel_pending_correction(self):
        self.engine.stop()  # nobody will pick the task up
        cid, _ = create_correction(self.db, self.batch2, 1, "abort",
                                   [{"action": "drop", "record_key": "PO-2003"}])
        self.engine.enqueue_correction(cid)
        ok, _ = self.engine.cancel_correction(cid)
        self.assertTrue(ok)
        c = self.q1("SELECT status FROM corrections WHERE id=?", (cid,))
        self.assertEqual(c["status"], "cancelled")
        b = self.q1("SELECT current_version FROM batches WHERE id=?", (self.batch2,))
        self.assertEqual(b["current_version"], 1)

    def test_cancel_running_correction_rolls_back(self):
        self.engine.stop()  # only the slow engine may claim tasks from here
        slow = Engine(self.db, workers=1, step_delay=0.5)
        slow.start()
        try:
            cid, _ = create_correction(self.db, self.batch2, 1, "abort mid-apply",
                                       [{"action": "drop", "record_key": "PO-2003"}])
            slow.enqueue_correction(cid)
            wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                     (cid,))["status"] == "running",
                     what="correction running")
            time.sleep(0.2)  # land the cancel inside apply's sleep window (0.5s)
            ok, _ = slow.cancel_correction(cid)
            self.assertTrue(ok)
            wait_for(lambda: self.q1("SELECT status FROM corrections WHERE id=?",
                                     (cid,))["status"] == "cancelled",
                     what="correction cancelled")
            # rolled back: version untouched, no v2 row, v1 still active
            b = self.q1("SELECT current_version FROM batches WHERE id=?", (self.batch2,))
            self.assertEqual(b["current_version"], 1)
            n = self.q1("SELECT COUNT(*) c FROM batch_versions WHERE batch_id=?",
                        (self.batch2,))["c"]
            self.assertEqual(n, 1)
            v1 = self.q1("SELECT state FROM batch_versions WHERE batch_id=? AND version=1",
                         (self.batch2,))
            self.assertEqual(v1["state"], "active")
        finally:
            slow.stop()

    def test_cancel_terminal_task_rejected(self):
        tid = self.q1("SELECT id FROM tasks WHERE status='done' LIMIT 1")["id"]
        ok, msg = self.engine.cancel_task(tid)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main(verbosity=2)
