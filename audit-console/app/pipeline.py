"""Task engine: queue, workers, correction application, impact-scope re-queue.

Design notes
------------
* A correction NEVER mutates history. It creates batch_version v+1 by copying
  the base version's files/records and applying correction_items, then marks
  the old version (and its entities) `superseded` — the original conclusions
  stay queryable forever (保留原结论).
* Version advance is a compare-and-swap inside BEGIN IMMEDIATE, so concurrent
  corrections on the same batch serialize: exactly one wins, the rest come out
  `conflicted` with a human-readable conflict_reason.
* Downstream tasks of the superseded version are re-queued by impact scope:
  running ones get a cooperative cancel request, queued ones are superseded,
  and a fresh extract -> aggregate chain is enqueued for the new version.
* All cancellation is cooperative and observable: queued tasks flip to
  `cancelled` immediately, running tasks finish their current step, notice the
  flag, and roll back.
"""
import json
import threading
import time

from .db import connect, dumps, loads, log_event, now

TERMINAL = ("done", "failed", "cancelled", "superseded")


class Engine:
    def __init__(self, db_path, workers=2, step_delay=0.25):
        self.db_path = db_path
        self.step_delay = step_delay          # simulated work per step; keeps cancellation observable
        self._stop = threading.Event()
        self._threads = []
        self._n_workers = workers
        self._wake = threading.Event()

    # ---------------------------------------------------------- lifecycle
    def start(self):
        for i in range(self._n_workers):
            t = threading.Thread(target=self._worker, name=f"worker-{i}", daemon=True)
            t.start()
            self._threads.append(t)

    def stop(self):
        self._stop.set()
        self._wake.set()
        for t in self._threads:
            t.join(timeout=5)

    # ---------------------------------------------------------- enqueue api
    @staticmethod
    def _insert_task(conn, task_type, batch_version_id=None, depends_on=(),
                     correction_id=None, supersedes=None):
        """Insert on an existing connection (safe inside a write transaction)."""
        cur = conn.execute(
            """INSERT INTO tasks(task_type, batch_version_id, correction_id,
                                 depends_on, supersedes, created_at)
               VALUES(?,?,?,?,?,?)""",
            (task_type, batch_version_id, correction_id,
             dumps(list(depends_on)), supersedes, now()),
        )
        log_event(conn, "task", f"任务#{cur.lastrowid}({task_type}) 入队",
                  "task", cur.lastrowid)
        return cur.lastrowid

    def enqueue(self, task_type, batch_version_id=None, depends_on=(),
                correction_id=None, supersedes=None):
        conn = connect(self.db_path)
        try:
            tid = self._insert_task(conn, task_type, batch_version_id,
                                    depends_on, correction_id, supersedes)
            conn.commit()
            self._wake.set()
            return tid
        finally:
            conn.close()

    def enqueue_ingest_chain(self, batch_version_id):
        """parse -> extract -> aggregate for a freshly ingested version."""
        p = self.enqueue("parse", batch_version_id)
        x = self.enqueue("extract", batch_version_id, depends_on=[p])
        a = self.enqueue("aggregate", batch_version_id, depends_on=[x])
        return [p, x, a]

    def enqueue_correction(self, correction_id):
        return self.enqueue("correction", correction_id=correction_id)

    # ---------------------------------------------------------- cancel api
    def cancel_task(self, task_id):
        """Queued -> cancelled now; running -> cooperative cancel request."""
        conn = connect(self.db_path)
        try:
            row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not row or row["status"] in TERMINAL:
                return False, "任务已结束，无法取消"
            if row["status"] == "queued":
                conn.execute(
                    "UPDATE tasks SET status='cancelled', cancel_requested=1,"
                    " finished_at=? WHERE id=?", (now(), task_id))
                log_event(conn, "task", f"任务#{task_id} 在排队中被取消", "task", task_id)
                self._cancel_dependents(conn, task_id, "上游任务被取消")
            else:
                conn.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (task_id,))
                log_event(conn, "task", f"任务#{task_id} 收到取消请求(运行中,协作式)",
                          "task", task_id)
            conn.commit()
            self._wake.set()
            return True, "已取消" if row["status"] == "queued" else "已发送取消请求"
        finally:
            conn.close()

    def cancel_correction(self, correction_id):
        conn = connect(self.db_path)
        try:
            c = conn.execute("SELECT * FROM corrections WHERE id=?",
                             (correction_id,)).fetchone()
            if not c or c["status"] not in ("pending", "running"):
                return False, "纠错已结束，无法取消"
            task = conn.execute(
                "SELECT id, status FROM tasks WHERE correction_id=? AND task_type='correction'"
                " ORDER BY id DESC LIMIT 1", (correction_id,)).fetchone()
            if task and task["status"] == "queued":
                conn.execute("UPDATE tasks SET status='cancelled', cancel_requested=1,"
                             " finished_at=? WHERE id=?", (now(), task["id"]))
                conn.execute("UPDATE corrections SET status='cancelled', finished_at=?"
                             " WHERE id=?", (now(), correction_id))
                log_event(conn, "correction", f"纠错#{correction_id} 在排队中被取消",
                          "correction", correction_id)
            elif task:  # running -> cooperative
                conn.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (task["id"],))
                log_event(conn, "correction", f"纠错#{correction_id} 收到取消请求(运行中)",
                          "correction", correction_id)
            conn.commit()
            return True, "ok"
        finally:
            conn.close()

    # ---------------------------------------------------------- worker loop
    def _worker(self):
        while not self._stop.is_set():
            task = self._claim()
            if task is None:
                self._wake.wait(timeout=0.2)
                self._wake.clear()
                continue
            self._run(task)

    def _claim(self):
        conn = connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM tasks WHERE status='queued' ORDER BY id").fetchall()
            for row in rows:
                deps = loads(row["depends_on"], [])
                if not deps:
                    chosen = row
                    break
                states = {r["status"] for r in conn.execute(
                    f"SELECT status FROM tasks WHERE id IN ({','.join('?'*len(deps))})",
                    deps).fetchall()}
                if states & {"failed", "cancelled", "superseded"}:
                    conn.execute(
                        "UPDATE tasks SET status='cancelled', finished_at=?, result=?"
                        " WHERE id=?",
                        (now(), dumps({"reason": "依赖任务未成功,本任务取消"}), row["id"]))
                    log_event(conn, "task",
                              f"任务#{row['id']} 因依赖未成功而取消", "task", row["id"])
                    conn.commit()
                    continue
                if states == {"done"}:
                    chosen = row
                    break
            else:
                return None
            cur = conn.execute(
                "UPDATE tasks SET status='running', started_at=? WHERE id=? AND status='queued'",
                (now(), chosen["id"]))
            conn.commit()
            if cur.rowcount == 0:
                return None
            return dict(chosen)
        finally:
            conn.close()

    def _run(self, task):
        tid = task["id"]
        conn = connect(self.db_path)
        try:
            handler = {
                "parse": self._do_parse,
                "extract": self._do_extract,
                "aggregate": self._do_aggregate,
                "correction": self._do_correction,
            }[task["task_type"]]
            result = handler(conn, task)
            status = "done"
        except Cancelled as c:
            conn.rollback()   # drop partial work; never commit half a task
            result = {"reason": str(c) or "已取消"}
            status = "cancelled"
        except Exception as e:  # noqa: BLE001 - surface the error to the audit log
            conn.rollback()
            result = {"error": f"{type(e).__name__}: {e}"}
            status = "failed"
        conn.execute(
            "UPDATE tasks SET status=?, result=?, finished_at=? WHERE id=?",
            (status, dumps(result), now(), tid))
        log_event(conn, "task", f"任务#{tid}({task['task_type']}) {status}: "
                                f"{json.dumps(result, ensure_ascii=False)[:200]}",
                  "task", tid)
        conn.commit()
        conn.close()
        self._wake.set()

    def _check_cancel(self, task_id):
        """Fresh-connection read: a worker conn inside a write txn has a fixed
        WAL snapshot and would never see a cancel committed after txn start."""
        conn = connect(self.db_path)
        try:
            row = conn.execute("SELECT cancel_requested FROM tasks WHERE id=?",
                               (task_id,)).fetchone()
        finally:
            conn.close()
        if row and row["cancel_requested"]:
            raise Cancelled("运行中被取消")

    def _step(self, task_id, n=1):
        for _ in range(n):
            time.sleep(self.step_delay)
            self._check_cancel(task_id)

    def _cancel_dependents(self, conn, task_id, reason):
        deps = conn.execute("SELECT id, depends_on, status FROM tasks"
                            " WHERE status='queued'").fetchall()
        for d in deps:
            if task_id in loads(d["depends_on"], []):
                conn.execute(
                    "UPDATE tasks SET status='cancelled', finished_at=?, result=?"
                    " WHERE id=?",
                    (now(), dumps({"reason": reason}), d["id"]))
                log_event(conn, "task", f"任务#{d['id']} 因{reason}而取消", "task", d["id"])
                self._cancel_dependents(conn, d["id"], reason)

    # ---------------------------------------------------------- task bodies
    def _do_parse(self, conn, task):
        bv = task["batch_version_id"]
        files = conn.execute("SELECT * FROM raw_files WHERE batch_version_id=?",
                             (bv,)).fetchall()
        n_ok = n_err = 0
        for f in files:
            self._step(task["id"])
            content = loads(f["payload"], {})
            for rec in content.get("records", []):
                status = rec.get("status", "ok")
                conn.execute(
                    """INSERT INTO parse_records(batch_version_id, raw_file_id, record_key,
                                                 status, payload, error_msg, created_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (bv, f["id"], rec["key"], status, dumps(rec.get("data", {})),
                     rec.get("error", ""), now()))
                n_ok, n_err = n_ok + (status != "error"), n_err + (status == "error")
        conn.execute(
            "UPDATE batches SET status=? WHERE id=(SELECT batch_id FROM batch_versions WHERE id=?)",
            ("failed" if n_err and not n_ok else "parsed", bv))
        conn.commit()
        return {"files": len(files), "records_ok": n_ok, "records_error": n_err}

    def _do_extract(self, conn, task):
        bv = task["batch_version_id"]
        recs = conn.execute(
            "SELECT * FROM parse_records WHERE batch_version_id=? AND status!='error'",
            (bv,)).fetchall()
        n = 0
        for r in recs:
            self._step(task["id"])
            payload = loads(r["payload"], {})
            etype = payload.get("type", "order")
            conn.execute(
                """INSERT INTO entities(batch_version_id, parse_record_id, entity_type,
                                        entity_key, payload, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (bv, r["id"], etype, f"{etype}:{r['record_key']}", r["payload"], now()))
            n += 1
        conn.commit()
        return {"entities": n}

    def _do_aggregate(self, conn, task):
        bv = task["batch_version_id"]
        self._step(task["id"], n=2)
        row = conn.execute(
            """SELECT s.id AS source_id, b.label FROM batch_versions bv
               JOIN batches b ON b.id=bv.batch_id JOIN sources s ON s.id=b.source_id
               WHERE bv.id=?""", (bv,)).fetchone()
        counts = {}
        for r in conn.execute(
                "SELECT entity_type, COUNT(*) c FROM entities WHERE batch_version_id=?"
                " AND state='active' GROUP BY entity_type", (bv,)).fetchall():
            counts[r["entity_type"]] = r["c"]
        conn.execute(
            """INSERT INTO entities(batch_version_id, parse_record_id, entity_type,
                                    entity_key, payload, created_at)
               VALUES(?,NULL,'source_rollup',?,?,?)""",
            (bv, f"rollup:batch:{bv}",
             dumps({"source_id": row["source_id"], "batch": row["label"],
                    "counts": counts}), now()))
        conn.commit()
        return {"rollup": counts}

    def _do_correction(self, conn, task):
        return apply_correction(conn, task["correction_id"], engine=self, task_id=task["id"])

    # ---------------------------------------------------------- re-queue
    def requeue_downstream(self, conn, old_bv, new_bv, affected_keys):
        """Impact-scope re-queue: kill/replace downstream tasks of old version."""
        requeued, killed = [], []
        old_tasks = conn.execute(
            """SELECT * FROM tasks WHERE batch_version_id=? AND task_type IN ('extract','aggregate')
               AND status IN ('queued','running')""", (old_bv,)).fetchall()
        old_extract = None
        for t in old_tasks:
            if t["status"] == "queued":
                conn.execute("UPDATE tasks SET status='superseded', finished_at=? WHERE id=?",
                             (now(), t["id"]))
            else:
                conn.execute("UPDATE tasks SET cancel_requested=1 WHERE id=?", (t["id"],))
            killed.append(t["id"])
            if t["task_type"] == "extract":
                old_extract = t["id"]
        # fresh chain for the new version, linked into the re-queue chain
        x = self._insert_task(conn, "extract", new_bv, supersedes=old_extract)
        a = self._insert_task(conn, "aggregate", new_bv, depends_on=[x])
        requeued = [x, a]
        return {"affected_record_keys": sorted(affected_keys),
                "superseded_or_cancelled_tasks": killed,
                "requeued_tasks": requeued}


class Cancelled(Exception):
    pass


# ---------------------------------------------------------------------------
# Correction application (atomic; safe under concurrency)
# ---------------------------------------------------------------------------
def create_correction(db_path, batch_id, base_version, reason, items, created_by="auditor"):
    """Validate + persist a correction; returns (correction_id, error)."""
    conn = connect(db_path)
    try:
        b = conn.execute("SELECT * FROM batches WHERE id=?", (batch_id,)).fetchone()
        if not b:
            return None, "批次不存在"
        if not items:
            return None, "纠错内容为空"
        for it in items:
            if it.get("action") not in ("fix", "drop", "add"):
                return None, f"未知动作: {it.get('action')}"
            if not it.get("record_key"):
                return None, "缺少 record_key"
        cur = conn.execute(
            """INSERT INTO corrections(batch_id, base_version, reason, created_by, created_at)
               VALUES(?,?,?,?,?)""",
            (batch_id, base_version, reason, created_by, now()))
        cid = cur.lastrowid
        for it in items:
            conn.execute(
                "INSERT INTO correction_items(correction_id, action, record_key, patch)"
                " VALUES(?,?,?,?)",
                (cid, it["action"], it["record_key"], dumps(it.get("patch", {}))))
        log_event(conn, "correction",
                  f"纠错#{cid} 创建: 批次#{batch_id} 基于 v{base_version}, {len(items)} 项",
                  "correction", cid)
        conn.commit()
        return cid, None
    finally:
        conn.close()


def apply_correction(conn, correction_id, engine, task_id=None):
    """Apply one correction atomically. Called by the correction task worker.

    Two phases:
      1. cooperative window (NO write lock held): the cancel flag can be
         committed by another connection and is observed at checkpoints —
         this is what makes cancelling a running correction effective.
      2. atomic write phase (BEGIN IMMEDIATE, no sleeps): conflict check +
         CAS on batches.current_version + all writes + commit. Concurrent
         applies serialize on the write lock; exactly one wins, the rest
         come out `conflicted`.
    """
    c = conn.execute("SELECT * FROM corrections WHERE id=?", (correction_id,)).fetchone()
    if not c:
        raise RuntimeError("纠错不存在")
    if c["status"] == "cancelled":
        raise Cancelled("纠错已取消")
    conn.execute("UPDATE corrections SET status='running' WHERE id=?", (correction_id,))
    conn.commit()

    items = conn.execute("SELECT * FROM correction_items WHERE correction_id=?",
                         (correction_id,)).fetchall()
    try:
        # phase 1: cooperative-cancel window, no write lock held
        if task_id:
            engine._step(task_id, n=1)

        # phase 2: atomic write — short, no sleeps, holds the lock briefly
        conn.execute("BEGIN IMMEDIATE")
        if task_id:
            engine._check_cancel(task_id)
        b = conn.execute("SELECT * FROM batches WHERE id=?", (c["batch_id"],)).fetchone()
        base_v = conn.execute(
            "SELECT * FROM batch_versions WHERE batch_id=? AND version=?",
            (c["batch_id"], c["base_version"])).fetchone()
        if b["current_version"] != c["base_version"] or base_v is None:
            winner = conn.execute(
                """SELECT correction_id FROM batch_versions
                   WHERE batch_id=? AND version=?""",
                (c["batch_id"], b["current_version"])).fetchone()
            reason = (f"版本冲突: 纠错基于 v{c['base_version']}, 但批次当前已是 "
                      f"v{b['current_version']}"
                      + (f"(被纠错#{winner['correction_id']} 推进)" if winner and
                         winner["correction_id"] else ""))
            conn.execute(
                """UPDATE corrections SET status='conflicted', conflict_reason=?,
                   finished_at=? WHERE id=?""", (reason, now(), correction_id))
            log_event(conn, "correction", f"纠错#{correction_id} 冲突: {reason}",
                      "correction", correction_id)
            conn.commit()
            return {"conflicted": True, "reason": reason}

        new_version = c["base_version"] + 1
        # compare-and-swap: this is what makes concurrent applies serialize
        cur = conn.execute(
            "UPDATE batches SET current_version=?, status='parsed' WHERE id=? AND current_version=?",
            (new_version, c["batch_id"], c["base_version"]))
        if cur.rowcount == 0:
            raise RuntimeError("CAS failed unexpectedly inside write transaction")

        # 1. new version row; old version -> superseded (kept, 原结论保留)
        cur = conn.execute(
            """INSERT INTO batch_versions(batch_id, version, state, origin, correction_id,
                                          note, created_at)
               VALUES(?,?, 'active','correction',?,?,?)""",
            (c["batch_id"], new_version, correction_id,
             f"纠错#{correction_id}: {c['reason']}", now()))
        new_bv = cur.lastrowid
        conn.execute("UPDATE batch_versions SET state='superseded' WHERE id=?",
                     (base_v["id"],))

        # 2. copy raw files
        file_map = {}
        for f in conn.execute("SELECT * FROM raw_files WHERE batch_version_id=?",
                              (base_v["id"],)).fetchall():
            nf = conn.execute(
                """INSERT INTO raw_files(batch_version_id, filename, checksum, size_bytes,
                                         payload, created_at)
                   VALUES(?,?,?,?,?,?)""",
                (new_bv, f["filename"], f["checksum"], f["size_bytes"], f["payload"], now()))
            file_map[f["id"]] = nf.lastrowid

        # 3. copy records, applying fix/drop; then adds
        fixes = {i["record_key"]: loads(i["patch"], {})
                 for i in items if i["action"] == "fix"}
        drops = {i["record_key"] for i in items if i["action"] == "drop"}
        adds = [i for i in items if i["action"] == "add"]
        affected = set(fixes) | drops | {i["record_key"] for i in adds}
        copied = 0
        any_file = next(iter(file_map.values()), None)
        for r in conn.execute("SELECT * FROM parse_records WHERE batch_version_id=?",
                              (base_v["id"],)).fetchall():
            if r["record_key"] in drops:
                continue
            payload, status, err = r["payload"], r["status"], r["error_msg"]
            if r["record_key"] in fixes:
                patch = fixes[r["record_key"]]
                merged = loads(r["payload"], {})
                merged.update(patch.get("payload", patch))
                payload, status, err = dumps(merged), patch.get("status", "ok"), ""
            conn.execute(
                """INSERT INTO parse_records(batch_version_id, raw_file_id, record_key,
                                             status, payload, error_msg, created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (new_bv, file_map.get(r["raw_file_id"], any_file), r["record_key"],
                 status, payload, err, now()))
            copied += 1
        for i in adds:
            patch = loads(i["patch"], {})
            conn.execute(
                """INSERT INTO parse_records(batch_version_id, raw_file_id, record_key,
                                             status, payload, error_msg, created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (new_bv, any_file, i["record_key"], patch.get("status", "ok"),
                 dumps(patch.get("payload", patch)), "", now()))

        # 4. supersede old entities (rows stay -> 原结论保留)
        n_ent = conn.execute(
            "UPDATE entities SET state='superseded' WHERE batch_version_id=? AND state='active'",
            (base_v["id"],)).rowcount

        # final cooperative-cancel checkpoint before the commit point
        if task_id:
            engine._check_cancel(task_id)

        # 5. impact-scope re-queue of downstream tasks
        impact = engine.requeue_downstream(conn, base_v["id"], new_bv, affected)
        impact.update({"new_version": new_version, "records_copied": copied,
                       "records_added": len(adds), "records_dropped": len(drops),
                       "records_fixed": len(fixes), "entities_superseded": n_ent})

        conn.execute(
            """UPDATE corrections SET status='done', result_version=?, impact=?, finished_at=?
               WHERE id=?""",
            (new_version, dumps(impact), now(), correction_id))
        log_event(conn, "correction",
                  f"纠错#{correction_id} 完成: 生成 v{new_version}, 影响记录 {len(affected)} 条,"
                  f" 重排队任务 {impact['requeued_tasks']}", "correction", correction_id)
        conn.commit()
        engine._wake.set()
        return impact
    except Cancelled:
        conn.rollback()
        conn.execute("UPDATE corrections SET status='cancelled', finished_at=? WHERE id=?",
                     (now(), correction_id))
        log_event(conn, "correction", f"纠错#{correction_id} 运行中被取消,已回滚",
                  "correction", correction_id)
        conn.commit()
        raise
    except Exception:
        conn.rollback()
        conn.execute("UPDATE corrections SET status='conflicted',"
                     " conflict_reason=COALESCE(conflict_reason,'应用失败,已回滚'),"
                     " finished_at=? WHERE id=?", (now(), correction_id))
        conn.commit()
        raise
