from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .audit import make_entry, utc_now
from .domain import ConflictError, NotFoundError
from .rules import ID_PREFIX, STATES


class Repository:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._create_schema()

    def _create_schema(self) -> None:
        statuses = ",".join("'" + s.replace("'", "''") + "'" for s in STATES)
        with self.conn:
            self.conn.executescript(f"""
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    quantity REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 1,
                    status TEXT NOT NULL CHECK(status IN ({statuses})),
                    version INTEGER NOT NULL DEFAULT 1,
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS ux_items_external_ref
                    ON items(external_ref) WHERE external_ref IS NOT NULL;
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open'
                        CHECK(status IN ('open','closed')),
                    external_ref TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, external_ref)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER NOT NULL,
                    actor TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    entry_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS emission_outlets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    outlet_code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    limit_value REAL NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitoring_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    field_no TEXT NOT NULL UNIQUE,
                    outlet_code TEXT,
                    measured_value REAL NOT NULL,
                    verdict TEXT,
                    batch_status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(batch_status IN ('pending','done','failed')),
                    source TEXT NOT NULL DEFAULT 'raw'
                        CHECK(source IN ('raw','calibrated')),
                    version INTEGER NOT NULL DEFAULT 1,
                    first_ingest_version INTEGER NOT NULL DEFAULT 1,
                    calibration_version INTEGER,
                    failure_reason TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calibrations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    field_no TEXT NOT NULL UNIQUE,
                    batch_id INTEGER NOT NULL REFERENCES monitoring_batches(id) ON DELETE CASCADE,
                    calibrated_value REAL NOT NULL,
                    basis_version INTEGER NOT NULL,
                    previous_verdict TEXT NOT NULL,
                    new_verdict TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS disposal_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT NOT NULL UNIQUE,
                    field_no TEXT NOT NULL,
                    batch_id INTEGER NOT NULL REFERENCES monitoring_batches(id) ON DELETE CASCADE,
                    outlet_code TEXT,
                    measured_value REAL NOT NULL,
                    limit_value REAL NOT NULL,
                    order_status TEXT NOT NULL DEFAULT 'issued'
                        CHECK(order_status IN ('issued','review')),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_monitoring_batches_status
                    ON monitoring_batches(batch_status);
                CREATE INDEX IF NOT EXISTS ix_disposal_orders_field_no
                    ON disposal_orders(field_no);
            """)

    @staticmethod
    def _item(row: sqlite3.Row) -> Dict[str, Any]:
        return dict(row)

    def create_item(self, title: str, description: str, severity: str,
                    quantity: float, threshold: float, external_ref: Optional[str],
                    actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO items(title, description, severity, quantity, threshold,
                       status, version, external_ref, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                    (title, description, severity, quantity, threshold, STATES[0], 1,
                     external_ref, actor, now, now),
                )
                item_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("external_ref已存在") from exc
        return self.get_item(item_id)

    def get_item(self, item_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("项目不存在")
        return self._item(row)

    def list_items(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM items"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [self._item(row) for row in rows]

    def transition_item(self, item_id: int, target: str, expected_version: int,
                        actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE items SET status=?, version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (target, now, item_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone()
                if exists is None:
                    raise NotFoundError("项目不存在")
                raise ConflictError("版本冲突，请刷新后重试")
        return self.get_item(item_id)

    def add_record(self, item_id: int, kind: str, detail: str, status: str,
                   external_ref: Optional[str], actor: str) -> Dict[str, Any]:
        now = utc_now()
        self.get_item(item_id)
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO records(item_id, kind, detail, status, external_ref,
                       created_by, created_at) VALUES(?,?,?,?,?,?,?)""",
                    (item_id, kind, detail, status, external_ref, actor, now),
                )
                record_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("记录唯一标识已存在") from exc
        with self._lock:
            row = self.conn.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        return dict(row)

    def list_records(self, item_id: int) -> List[Dict[str, Any]]:
        self.get_item(item_id)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM records WHERE item_id=? ORDER BY id", (item_id,)
            ).fetchall()
        return [dict(row) for row in rows]

    def open_record_count(self, item_id: int) -> int:
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM records WHERE item_id=? AND status='open'",
                (item_id,),
            ).fetchone()
        return int(row["n"])

    def append_audit(self, action: str, entity_type: str, entity_id: int,
                     actor: str, detail: dict, in_tx: bool = False) -> Dict[str, Any]:
        event = make_entry(action, entity_type, entity_id, actor, detail,
                           self._previous_audit_hash())
        row_values = (event["action"], event["entity_type"], event["entity_id"],
                      event["actor"],
                      json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                      event["previous_hash"], event["entry_hash"],
                      event["created_at"])
        if in_tx:
            # 调用方已持有锁与事务：审计与业务变更原子提交。
            self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor,
                   detail, previous_hash, entry_hash, created_at)
                   VALUES(?,?,?,?,?,?,?,?)""", row_values)
        else:
            with self._lock, self.conn:
                self.conn.execute(
                    """INSERT INTO audit_events(action, entity_type, entity_id, actor,
                       detail, previous_hash, entry_hash, created_at)
                       VALUES(?,?,?,?,?,?,?,?)""", row_values)
        event["id"] = self.conn.execute(
            "SELECT id FROM audit_events WHERE entry_hash=?", (event["entry_hash"],)
        ).fetchone()[0]
        return event

    def _previous_audit_hash(self) -> str:
        row = self.conn.execute(
            "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return row["entry_hash"] if row else "GENESIS"

    def list_audit(self, entity_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM audit_events"
        params: tuple = ()
        if entity_id is not None:
            sql += " WHERE entity_id=?"
            params = (entity_id,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["detail"] = json.loads(item["detail"])
            result.append(item)
        return result

    def verify_audit_chain(self) -> bool:
        from .audit import calculate_hash
        with self._lock:
            rows = self.conn.execute("SELECT * FROM audit_events ORDER BY id").fetchall()
        previous = "GENESIS"
        for row in rows:
            if row["previous_hash"] != previous:
                return False
            payload = {
                "action": row["action"], "entity_type": row["entity_type"],
                "entity_id": row["entity_id"], "actor": row["actor"],
                "detail": json.loads(row["detail"]), "created_at": row["created_at"],
            }
            if calculate_hash(previous, payload) != row["entry_hash"]:
                return False
            previous = row["entry_hash"]
        return True

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    # ------------------------------------------------------------------
    # 排放口 / 监测批次 / 校准 / 处置单
    # ------------------------------------------------------------------
    def create_outlet(self, outlet_code: str, name: str, limit_value: float,
                      actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO emission_outlets(outlet_code, name, limit_value,
                       created_by, created_at) VALUES(?,?,?,?,?)""",
                    (outlet_code, name, limit_value, actor, now),
                )
                outlet_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("排放口编号已存在") from exc
        return self.get_outlet(outlet_id)

    def get_outlet(self, outlet_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM emission_outlets WHERE id=?", (outlet_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("排放口不存在")
        return dict(row)

    def get_outlet_by_code(self, outlet_code: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM emission_outlets WHERE outlet_code=?", (outlet_code,)
            ).fetchone()
        return dict(row) if row else None

    def list_outlets(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM emission_outlets ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def ingest_batch_tx(self, field_no: str, outlet_code: Optional[str],
                        measured_value: float, limit_value: Optional[float],
                        expected: Optional[int], actor: str,
                        audit=None, outcome: Optional[Dict[str, Any]] = None
                        ) -> Dict[str, Any]:
        """一个持锁事务内完成：并发裁决、插入（或补传/接续）、判值、发单、审计。

        全程持有同一连接上的锁，避免跨方法临界区读到其它事务未提交的行。
        并发语义由调用方约定：``expected`` 为 None 表示新建，给出整数表示
        同号补传/接续，且必须等于当前版本。
        ``audit`` 为 (action, detail) 列表，在提交前写入，与业务原子提交。
        """
        now = utc_now()
        batch_id = 0
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE field_no=?", (field_no,)
            ).fetchone()
            existing = dict(row) if row else None
            if existing is None:
                if expected is not None:
                    raise NotFoundError("监测批次不存在")
                if limit_value is None:
                    cur = self.conn.execute(
                        """INSERT INTO monitoring_batches(field_no, outlet_code,
                           measured_value, verdict, batch_status, source, version,
                           first_ingest_version, calibration_version, failure_reason,
                           created_by, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (field_no, outlet_code, measured_value, None, "failed", "raw",
                         1, 1, None, "排放口不存在或未登记", actor, now, now),
                    )
                    batch_id = int(cur.lastrowid)
                    batch = dict(self.conn.execute(
                        "SELECT * FROM monitoring_batches WHERE id=?",
                        (batch_id,)).fetchone())
                    if outcome is not None:
                        outcome.update(state="failed", order=None)
                    self._append_audits(audit, batch_id, actor)
                    return {"outcome": "created", "batch": batch, "order": None}
                verdict = "exceedance" if measured_value > limit_value else "compliant"
                cur = self.conn.execute(
                    """INSERT INTO monitoring_batches(field_no, outlet_code,
                       measured_value, verdict, batch_status, source, version,
                       first_ingest_version, calibration_version, created_by,
                       created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (field_no, outlet_code, measured_value, verdict, "done", "raw", 2,
                     1, None, actor, now, now),
                )
                batch_id = int(cur.lastrowid)
                order = None
                if verdict == "exceedance":
                    order = self._issue_order_in_tx(
                        field_no, batch_id, outlet_code, measured_value,
                        limit_value, now, actor)
                batch = dict(self.conn.execute(
                    "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
                ).fetchone())
                if outcome is not None:
                    outcome.update(state="created", order=order)
                self._append_audits(audit, batch_id, actor)
                return {"outcome": "created", "batch": batch, "order": order}

            # 已存在同号批次：只接受当前版本，后到者（无版本或旧版本）收到冲突。
            if expected is None or expected != existing["version"]:
                raise ConflictError("该批次已存在且版本不匹配，请刷新后重试")
            batch_id = existing["id"]
            if existing["batch_status"] == "failed":
                if limit_value is None:
                    self.conn.execute(
                        """UPDATE monitoring_batches SET failure_reason=?, updated_at=?
                           WHERE id=?""",
                        ("排放口仍未登记", now, batch_id),
                    )
                    batch = dict(self.conn.execute(
                        "SELECT * FROM monitoring_batches WHERE id=?",
                        (batch_id,)).fetchone())
                    if outcome is not None:
                        outcome.update(state="waiting", order=None)
                    self._append_audits(audit, batch_id, actor)
                    return {"outcome": "waiting", "batch": batch, "order": None}
                verdict = ("exceedance"
                           if existing["measured_value"] > limit_value
                           else "compliant")
                self.conn.execute(
                    """UPDATE monitoring_batches SET verdict=?, source='raw',
                       batch_status='done', failure_reason=NULL, version=version+1,
                       updated_at=? WHERE id=?""",
                    (verdict, now, batch_id),
                )
                order = None
                if verdict == "exceedance":
                    order = self._issue_order_in_tx(
                        field_no, batch_id, outlet_code,
                        existing["measured_value"], limit_value, now, actor)
                batch = dict(self.conn.execute(
                    "SELECT * FROM monitoring_batches WHERE id=?",
                    (batch_id,)).fetchone())
                if outcome is not None:
                    outcome.update(state="resumed", order=order)
                self._append_audits(audit, batch_id, actor)
                return {"outcome": "resumed", "batch": batch, "order": order}
            # 同号补传：行不变，已发出处置单继续有效。
            order_row = self.conn.execute(
                "SELECT * FROM disposal_orders WHERE field_no=? ORDER BY id LIMIT 1",
                (field_no,),
            ).fetchone()
            order = dict(order_row) if order_row else None
            if outcome is not None:
                outcome.update(state="reuploaded", order=order)
            self._append_audits(audit, batch_id, actor)
            return {"outcome": "reuploaded", "batch": existing,
                    "order": order}

    def _append_audits(self, audit, entity_id: int, actor: str) -> None:
        if audit is None:
            return
        if callable(audit):
            audit = audit(entity_id)
        for item in audit or []:
            self.append_audit(item["action"], "排放监测", entity_id, actor,
                              item["detail"], in_tx=True)

    def retry_batch_tx(self, batch_id: int, limit_value: float,
                       actor: str, audit=None,
                       outcome: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """持锁事务内把一个失败批次判值完成，必要时发处置单。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("监测批次不存在")
            batch = dict(row)
            if batch["batch_status"] != "failed":
                raise ConflictError("批次不在失败状态，无需接续处理")
            verdict = ("exceedance" if batch["measured_value"] > limit_value
                       else "compliant")
            self.conn.execute(
                """UPDATE monitoring_batches SET verdict=?, source='raw',
                   batch_status='done', failure_reason=NULL, version=version+1,
                   updated_at=? WHERE id=?""",
                (verdict, now, batch_id),
            )
            order = None
            if verdict == "exceedance":
                order = self._issue_order_in_tx(
                    batch["field_no"], batch_id, batch["outlet_code"],
                    batch["measured_value"], limit_value, now, actor)
            batch = dict(self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
            ).fetchone())
            if outcome is not None:
                outcome.update(verdict=verdict, order=order)
            self._append_audits(audit, batch_id, actor)
            return {"batch": batch, "order": order}

    def _issue_order_in_tx(self, field_no: str, batch_id: int,
                           outlet_code: Optional[str], measured_value: float,
                           limit_value: float, now: str,
                           actor: str) -> Optional[Dict[str, Any]]:
        existing = self.conn.execute(
            "SELECT id FROM disposal_orders WHERE field_no=?", (field_no,)
        ).fetchone()
        if existing is None:
            self.conn.execute(
                """INSERT INTO disposal_orders(order_no, field_no, batch_id,
                   outlet_code, measured_value, limit_value, order_status,
                   created_by, created_at, updated_at)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (f"DO-{field_no}", field_no, batch_id, outlet_code, measured_value,
                 limit_value, "issued", actor, now, now),
            )
        row = self.conn.execute(
            "SELECT * FROM disposal_orders WHERE field_no=?", (field_no,)
        ).fetchone()
        return dict(row) if row else None

    def get_batch_by_id(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("监测批次不存在")
        return dict(row)

    def find_batch_by_field_no(self, field_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE field_no=?", (field_no,)
            ).fetchone()
        return dict(row) if row else None

    def require_batch_by_field_no(self, field_no: str) -> Dict[str, Any]:
        batch = self.find_batch_by_field_no(field_no)
        if batch is None:
            raise NotFoundError("监测批次不存在")
        return batch

    def list_batches(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM monitoring_batches"
        params: tuple = ()
        if status:
            sql += " WHERE batch_status=?"
            params = (status,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def list_failed_batches(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE batch_status='failed' ORDER BY id"
            ).fetchall()
        return [dict(row) for row in rows]

    def apply_calibration(self, field_no: str, calibrated_value: float,
                          basis_version: int, previous_verdict: str, new_verdict: str,
                          expected_version: int, actor: str, audit=None) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE monitoring_batches SET measured_value=?, verdict=?,
                   source='calibrated', calibration_version=?, version=version+1,
                   updated_at=? WHERE field_no=? AND version=?""",
                (calibrated_value, new_verdict, basis_version, now,
                 field_no, expected_version),
            )
            if cur.rowcount == 0:
                batch = self.require_batch_by_field_no(field_no)
                raise ConflictError("版本冲突，请刷新后重试")
            batch = self.require_batch_by_field_no(field_no)
            try:
                self.conn.execute(
                    """INSERT INTO calibrations(field_no, batch_id, calibrated_value,
                       basis_version, previous_verdict, new_verdict, created_by, created_at)
                       VALUES(?,?,?,?,?,?,?,?)""",
                    (field_no, batch["id"], calibrated_value, basis_version,
                     previous_verdict, new_verdict, actor, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ConflictError("该现场单号已完成校准") from exc
            cur = self.conn.execute(
                """UPDATE disposal_orders SET order_status='review', updated_at=?
                   WHERE field_no=? AND order_status='issued'""",
                (now, field_no),
            )
            returned = cur.rowcount
            cal_row = self.conn.execute(
                "SELECT * FROM calibrations WHERE field_no=?", (field_no,)
            ).fetchone()
            if isinstance(audit, dict):
                audit["orders_returned"] = int(returned)
            self._append_audits(audit, batch["id"], actor)
        result = self.require_batch_by_field_no(field_no)
        result["_orders_returned"] = int(returned)
        result["_calibration"] = dict(cal_row)
        return result

    def touch_failed_batch_tx(self, batch_id: int, reason: str, actor: str,
                              audit=None) -> Dict[str, Any]:
        """排放口仍缺失时，在同一事务里刷新失败原因并记审计，不改判值。"""
        now = utc_now()
        with self._lock, self.conn:
            self.conn.execute(
                """UPDATE monitoring_batches SET failure_reason=?, updated_at=?
                   WHERE id=? AND batch_status='failed'""",
                (reason, now, batch_id),
            )
            batch = dict(self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
            ).fetchone())
            self._append_audits(audit, batch_id, actor)
            return batch

    def list_orders(self, field_no: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM disposal_orders"
        params: tuple = ()
        if field_no is not None:
            sql += " WHERE field_no=?"
            params = (field_no,)
        sql += " ORDER BY id"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def link_by_field_no(self, field_no: str) -> Dict[str, Any]:
        with self._lock:
            batch_row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE field_no=?", (field_no,)
            ).fetchone()
            if batch_row is None:
                raise NotFoundError("现场单号不存在")
            batch = dict(batch_row)
            outlet = None
            if batch["outlet_code"]:
                row = self.conn.execute(
                    "SELECT * FROM emission_outlets WHERE outlet_code=?",
                    (batch["outlet_code"],),
                ).fetchone()
                outlet = dict(row) if row else None
            cal_row = self.conn.execute(
                "SELECT * FROM calibrations WHERE field_no=?", (field_no,)
            ).fetchone()
            order_rows = self.conn.execute(
                "SELECT * FROM disposal_orders WHERE field_no=? ORDER BY id", (field_no,)
            ).fetchall()
        return {
            "field_no": field_no,
            "outlet": outlet,
            "batch": batch,
            "calibration": dict(cal_row) if cal_row else None,
            "orders": [dict(row) for row in order_rows],
        }
