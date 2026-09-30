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
                CREATE TABLE IF NOT EXISTS outlets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    pollutant TEXT NOT NULL,
                    limit_value REAL NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitoring_batches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_no TEXT NOT NULL UNIQUE,
                    outlet_code TEXT NOT NULL,
                    outlet_id INTEGER REFERENCES outlets(id),
                    status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(status IN ('pending','processed','failed')),
                    conclusion TEXT CHECK(conclusion IN ('exceeded','compliant')),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS monitoring_readings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id INTEGER NOT NULL
                        REFERENCES monitoring_batches(id) ON DELETE CASCADE,
                    pollutant TEXT NOT NULL,
                    raw_value REAL NOT NULL,
                    calibrated_value REAL,
                    calibration_version INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(batch_id, pollutant)
                );
                CREATE TABLE IF NOT EXISTS disposal_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_no TEXT NOT NULL UNIQUE,
                    batch_id INTEGER NOT NULL REFERENCES monitoring_batches(id),
                    outlet_id INTEGER NOT NULL REFERENCES outlets(id),
                    conclusion TEXT NOT NULL CHECK(conclusion IN ('exceeded','compliant')),
                    status TEXT NOT NULL DEFAULT 'issued'
                        CHECK(status IN ('issued','review','closed')),
                    version INTEGER NOT NULL DEFAULT 1,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS calibration_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reading_id INTEGER NOT NULL
                        REFERENCES monitoring_readings(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    value REAL NOT NULL,
                    note TEXT,
                    recorded_at TEXT NOT NULL,
                    UNIQUE(reading_id, version)
                );
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
                     actor: str, detail: dict) -> Dict[str, Any]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT entry_hash FROM audit_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            previous = row["entry_hash"] if row else "GENESIS"
            event = make_entry(action, entity_type, entity_id, actor, detail, previous)
            cur = self.conn.execute(
                """INSERT INTO audit_events(action, entity_type, entity_id, actor, detail,
                   previous_hash, entry_hash, created_at) VALUES(?,?,?,?,?,?,?,?)""",
                (event["action"], event["entity_type"], event["entity_id"], event["actor"],
                 json.dumps(event["detail"], ensure_ascii=False, sort_keys=True),
                 event["previous_hash"], event["entry_hash"], event["created_at"]),
            )
            event_id = int(cur.lastrowid)
        event["id"] = event_id
        return event

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

    # ---- 排放口 ----
    def create_outlet(self, code: str, name: str, pollutant: str,
                      limit_value: float, actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO outlets(code, name, pollutant, limit_value,
                       created_by, created_at) VALUES(?,?,?,?,?,?)""",
                    (code, name, pollutant, limit_value, actor, now),
                )
                outlet_id = int(cur.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ConflictError("排放口编号已存在") from exc
        return self.get_outlet(outlet_id)

    def get_outlet(self, outlet_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM outlets WHERE id=?", (outlet_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("排放口不存在")
        return dict(row)

    def get_outlet_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM outlets WHERE code=?", (code,)
            ).fetchone()
        return dict(row) if row else None

    def list_outlets(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute("SELECT * FROM outlets ORDER BY id").fetchall()
        return [dict(row) for row in rows]

    # ---- 监测批次 ----
    def create_batch(self, batch_no: str, outlet_code: str,
                     readings: List[Dict[str, Any]], actor: str) -> Dict[str, Any]:
        now = utc_now()
        try:
            with self._lock, self.conn:
                cur = self.conn.execute(
                    """INSERT INTO monitoring_batches(batch_no, outlet_code, status,
                       version, created_by, created_at, updated_at)
                       VALUES(?,?,?,?,?,?,?)""",
                    (batch_no, outlet_code, "pending", 1, actor, now, now),
                )
                batch_id = int(cur.lastrowid)
                for reading in readings:
                    self.conn.execute(
                        """INSERT INTO monitoring_readings(batch_id, pollutant, raw_value,
                           calibration_version, created_at, updated_at)
                           VALUES(?,?,?,?,?,?)""",
                        (batch_id, reading["pollutant"], reading["value"], 0, now, now),
                    )
        except sqlite3.IntegrityError as exc:
            raise ConflictError("现场单号已存在") from exc
        return self.get_batch(batch_id)

    def get_batch(self, batch_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE id=?", (batch_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("监测批次不存在")
        return dict(row)

    def get_batch_by_no(self, batch_no: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM monitoring_batches WHERE batch_no=?", (batch_no,)
            ).fetchone()
        return dict(row) if row else None

    def list_batches(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM monitoring_batches"
        params: tuple = ()
        if status:
            sql += " WHERE status=?"
            params = (status,)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def list_readings(self, batch_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM monitoring_readings WHERE batch_id=? ORDER BY id",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def get_reading(self, reading_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM monitoring_readings WHERE id=?", (reading_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("监测读数不存在")
        return dict(row)

    def update_batch_readings(self, batch_id: int, readings: List[Dict[str, Any]],
                              expected_version: int, actor: str) -> Dict[str, Any]:
        """乐观锁更新批次读数，后到的版本冲突。校准值保留不动。"""
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE monitoring_batches SET version=version+1, updated_at=?
                   WHERE id=? AND version=?""",
                (now, batch_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM monitoring_batches WHERE id=?", (batch_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("监测批次不存在")
                raise ConflictError("批次版本冲突，请刷新后重试")
            for reading in readings:
                self.conn.execute(
                    """INSERT INTO monitoring_readings(batch_id, pollutant, raw_value,
                       calibration_version, created_at, updated_at)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(batch_id, pollutant) DO UPDATE SET
                         raw_value=excluded.raw_value, updated_at=excluded.updated_at""",
                    (batch_id, reading["pollutant"], reading["value"], 0, now, now),
                )
        return self.get_batch(batch_id)

    def process_batch(self, batch_id: int, status: str, conclusion: Optional[str],
                      outlet_id: Optional[int], expected_version: int,
                      actor: str, order_no: Optional[str] = None) -> Dict[str, Any]:
        """乐观锁判定批次：pending/failed -> processed（失败保留为 failed）。
        超标时原子补开处置单。"""
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE monitoring_batches SET status=?, conclusion=?, outlet_id=?,
                   version=version+1, updated_at=? WHERE id=? AND version=?""",
                (status, conclusion, outlet_id, now, batch_id, expected_version),
            )
            if cur.rowcount == 0:
                exists = self.conn.execute(
                    "SELECT 1 FROM monitoring_batches WHERE id=?", (batch_id,)
                ).fetchone()
                if exists is None:
                    raise NotFoundError("监测批次不存在")
                raise ConflictError("批次版本冲突，请刷新后重试")
            if conclusion == "exceeded" and order_no is not None and outlet_id is not None:
                existing = self.conn.execute(
                    "SELECT 1 FROM disposal_orders WHERE batch_id=? LIMIT 1",
                    (batch_id,),
                ).fetchone()
                if existing is None:
                    self.conn.execute(
                        """INSERT INTO disposal_orders(order_no, batch_id, outlet_id,
                           conclusion, status, version, created_by, created_at, updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (order_no, batch_id, outlet_id, conclusion, "issued", 1,
                         actor, now, now),
                    )
        return self.get_batch(batch_id)

    def apply_calibration(self, reading_id: int, calibrated_value: float,
                          new_conclusion: str, changed: bool, actor: str,
                          order_no: Optional[str] = None) -> Dict[str, Any]:
        """晚到校准入库：补记首次入库版本、写校准版本、重算批次结论、
        原处置单退回复核。判定结论由 service 层 rules 计算后传入。"""
        now = utc_now()
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM monitoring_readings WHERE id=?", (reading_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("监测读数不存在")
            reading = dict(row)
            current_version = reading["calibration_version"]
            # 旧数据没有校准版本时，补记首次入库版本
            if current_version == 0:
                self.conn.execute(
                    """INSERT INTO calibration_versions(reading_id, version, value, note, recorded_at)
                       VALUES(?,?,?,?,?)""",
                    (reading_id, 1, reading["raw_value"], "首次入库版本（补记）", now),
                )
                next_version = 2
            else:
                next_version = current_version + 1
            self.conn.execute(
                """INSERT INTO calibration_versions(reading_id, version, value, note, recorded_at)
                   VALUES(?,?,?,?,?)""",
                (reading_id, next_version, calibrated_value, "校准值", now),
            )
            self.conn.execute(
                """UPDATE monitoring_readings SET calibrated_value=?, calibration_version=?,
                   updated_at=? WHERE id=?""",
                (calibrated_value, next_version, now, reading_id),
            )
            batch_id = reading["batch_id"]
            if changed:
                self.conn.execute(
                    """UPDATE monitoring_batches SET conclusion=?, version=version+1,
                       updated_at=? WHERE id=?""",
                    (new_conclusion, now, batch_id),
                )
                # 旧超标结论失效，原处置单退回复核
                self.conn.execute(
                    """UPDATE disposal_orders SET status='review', version=version+1,
                       updated_at=? WHERE batch_id=? AND status='issued'""",
                    (now, batch_id),
                )
                # 重算后超标且无在效处置单时补开
                if new_conclusion == "exceeded" and order_no is not None:
                    active = self.conn.execute(
                        "SELECT 1 FROM disposal_orders WHERE batch_id=? AND status IN ('issued','review') LIMIT 1",
                        (batch_id,),
                    ).fetchone()
                    if active is None:
                        batch_row = self.conn.execute(
                            "SELECT outlet_id FROM monitoring_batches WHERE id=?",
                            (batch_id,),
                        ).fetchone()
                        self.conn.execute(
                            """INSERT INTO disposal_orders(order_no, batch_id, outlet_id,
                               conclusion, status, version, created_by, created_at, updated_at)
                               VALUES(?,?,?,?,?,?,?,?,?)""",
                            (order_no, batch_id, batch_row["outlet_id"], new_conclusion,
                             "issued", 1, actor, now, now),
                        )
        return {
            "reading": self.get_reading(reading_id),
            "batch": self.get_batch(batch_id),
            "versions": self.list_calibration_versions(reading_id),
            "conclusion_changed": changed,
        }

    def list_calibration_versions(self, reading_id: int) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM calibration_versions WHERE reading_id=? ORDER BY version",
                (reading_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ---- 处置单 ----
    def get_order(self, order_id: int) -> Dict[str, Any]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM disposal_orders WHERE id=?", (order_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("处置单不存在")
        return dict(row)

    def list_orders(self, status: Optional[str] = None,
                    batch_id: Optional[int] = None) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM disposal_orders"
        conditions: List[str] = []
        params: List[Any] = []
        if status:
            conditions.append("status=?")
            params.append(status)
        if batch_id is not None:
            conditions.append("batch_id=?")
            params.append(batch_id)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id DESC"
        with self._lock:
            rows = self.conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def update_order_status(self, order_id: int, status: str,
                            actor: str) -> Dict[str, Any]:
        now = utc_now()
        with self._lock, self.conn:
            cur = self.conn.execute(
                """UPDATE disposal_orders SET status=?, version=version+1, updated_at=?
                   WHERE id=?""",
                (status, now, order_id),
            )
            if cur.rowcount == 0:
                raise NotFoundError("处置单不存在")
        return self.get_order(order_id)
