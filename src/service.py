from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import ensure_role, normalize_severity, require_number, require_text
from .repository import Repository
from .rules import (AUDIT_ROLES, BATCH_PROCESS_ROLES, BATCH_SUBMIT_ROLES,
                    CALIBRATE_ROLES, CREATE_ROLES, ENTITY, ORDER_REVIEW_ROLES,
                    OUTLET_ROLES, RECORD_ROLES, TITLE, VIEW_ROLES,
                    completion_blockers, conclusion_changed, escalation_required,
                    judge_batch_readings, priority_score, response_deadline_hours,
                    role_for_transition, validate_transition)


class Service:
    def __init__(self, repository: Repository):
        self.repository = repository

    def _view(self, role: str) -> None:
        ensure_role(role, VIEW_ROLES)

    def create_item(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, CREATE_ROLES)
        actor = require_text(actor, "actor", 100)
        title = require_text(payload.get("title"), "title", 200)
        description = require_text(payload.get("description"), "description")
        severity = normalize_severity(payload.get("severity"))
        quantity = require_number(payload.get("quantity", 0), "quantity")
        threshold = require_number(payload.get("threshold", 1), "threshold", 0.000001)
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        item = self.repository.create_item(title, description, severity, quantity,
                                           threshold, external_ref, actor)
        self.repository.append_audit("create", ENTITY, item["id"], actor, {
            "title": title, "severity": severity, "quantity": quantity,
            "priority": priority_score(severity, quantity, threshold),
        })
        return self.enrich(item)

    def add_record(self, item_id: int, payload: Dict[str, Any], actor: str,
                   role: str) -> Dict[str, Any]:
        ensure_role(role, RECORD_ROLES)
        actor = require_text(actor, "actor", 100)
        kind = require_text(payload.get("kind"), "kind", 100)
        detail = require_text(payload.get("detail"), "detail")
        status = payload.get("status", "open")
        if status not in ("open", "closed"):
            raise ValueError("status必须是open或closed")
        external_ref = payload.get("external_ref")
        if external_ref is not None:
            external_ref = require_text(external_ref, "external_ref", 100)
        record = self.repository.add_record(item_id, kind, detail, status,
                                            external_ref, actor)
        self.repository.append_audit("record", ENTITY, item_id, actor, {
            "record_id": record["id"], "kind": kind, "status": status,
        })
        return record

    def transition(self, item_id: int, target: str, expected_version: int,
                   actor: str, role: str) -> Dict[str, Any]:
        actor = require_text(actor, "actor", 100)
        item = self.repository.get_item(item_id)
        validate_transition(item["status"], target)
        ensure_role(role, role_for_transition(target))
        if not isinstance(expected_version, int) or expected_version < 1:
            raise ValueError("expected_version必须是正整数")
        blockers = completion_blockers(target, self.repository.open_record_count(item_id))
        if blockers:
            from .domain import ConflictError
            raise ConflictError("；".join(blockers))
        updated = self.repository.transition_item(item_id, target, expected_version, actor)
        self.repository.append_audit("transition", ENTITY, item_id, actor, {
            "from": item["status"], "to": target,
            "escalation_required": escalation_required(
                item["severity"], item["quantity"], item["threshold"]),
        })
        return self.enrich(updated)

    def get_item(self, item_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.enrich(self.repository.get_item(item_id))

    def list_items(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self.enrich(item) for item in self.repository.list_items(status)]

    def list_records(self, item_id: int, role: str) -> list:
        self._view(role)
        return self.repository.list_records(item_id)

    def audit(self, role: str, item_id: Optional[int] = None) -> list:
        ensure_role(role, AUDIT_ROLES)
        return self.repository.list_audit(item_id)

    # ---- 排放口 ----
    def create_outlet(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, OUTLET_ROLES)
        actor = require_text(actor, "actor", 100)
        code = require_text(payload.get("code"), "code", 100)
        name = require_text(payload.get("name"), "name", 200)
        pollutant = require_text(payload.get("pollutant"), "pollutant", 100)
        limit_value = require_number(payload.get("limit_value"), "limit_value", 0.000001)
        outlet = self.repository.create_outlet(code, name, pollutant, limit_value, actor)
        self.repository.append_audit("outlet_create", "outlet", outlet["id"], actor, {
            "code": code, "pollutant": pollutant, "limit_value": limit_value,
        })
        return outlet

    def list_outlets(self, role: str) -> list:
        self._view(role)
        return self.repository.list_outlets()

    def get_outlet(self, outlet_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.repository.get_outlet(outlet_id)

    # ---- 监测批次 ----
    @staticmethod
    def _validate_readings(readings: Any) -> list:
        if not isinstance(readings, list) or not readings:
            from .domain import ValidationError
            raise ValidationError("readings必须是非空列表")
        result = []
        for reading in readings:
            if not isinstance(reading, dict):
                from .domain import ValidationError
                raise ValidationError("reading必须是对象")
            pollutant = require_text(reading.get("pollutant"), "pollutant", 100)
            value = require_number(reading.get("value"), "value")
            result.append({"pollutant": pollutant, "value": value})
        return result

    def _assemble_batch(self, batch: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(batch)
        result["readings"] = self.repository.list_readings(batch["id"])
        result["orders"] = self.repository.list_orders(batch_id=batch["id"])
        return result

    def submit_batch(self, payload: Dict[str, Any], actor: str, role: str) -> Dict[str, Any]:
        """按现场单号提交监测批次。同号补传沿用第一次判值，不重复判定。"""
        ensure_role(role, BATCH_SUBMIT_ROLES)
        actor = require_text(actor, "actor", 100)
        batch_no = require_text(payload.get("batch_no"), "batch_no", 100)
        outlet_code = require_text(payload.get("outlet_code"), "outlet_code", 100)
        readings = self._validate_readings(payload.get("readings"))
        existing = self.repository.get_batch_by_no(batch_no)
        if existing is not None:
            result = self._assemble_batch(existing)
            result["existed"] = True
            return result
        batch = self.repository.create_batch(batch_no, outlet_code, readings, actor)
        self.repository.append_audit("batch_submit", "monitoring_batch", batch["id"], actor, {
            "batch_no": batch_no, "outlet_code": outlet_code, "readings": len(readings),
        })
        result = self._assemble_batch(batch)
        result["existed"] = False
        return result

    def list_batches(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return [self._assemble_batch(batch)
                for batch in self.repository.list_batches(status)]

    def get_batch(self, batch_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self._assemble_batch(self.repository.get_batch(batch_id))

    def update_readings(self, batch_id: int, payload: Dict[str, Any],
                        actor: str, role: str) -> Dict[str, Any]:
        """乐观锁更新批次读数，后到的版本收到冲突。"""
        ensure_role(role, BATCH_PROCESS_ROLES)
        actor = require_text(actor, "actor", 100)
        readings = self._validate_readings(payload.get("readings"))
        expected_version = payload.get("expected_version")
        if not isinstance(expected_version, int) or expected_version < 1:
            from .domain import ValidationError
            raise ValidationError("expected_version必须是正整数")
        batch = self.repository.update_batch_readings(batch_id, readings, expected_version, actor)
        self.repository.append_audit("batch_readings_update", "monitoring_batch", batch_id, actor, {
            "readings": len(readings),
        })
        return self._assemble_batch(batch)

    def process_batch(self, batch_id: int, payload: Dict[str, Any],
                      actor: str, role: str) -> Dict[str, Any]:
        """判定批次：pending/failed -> processed。失败批次保留后接着处理。
        两名值班员同时提交时只接受当前版本，后到的收到冲突。"""
        ensure_role(role, BATCH_PROCESS_ROLES)
        actor = require_text(actor, "actor", 100)
        expected_version = payload.get("expected_version")
        if not isinstance(expected_version, int) or expected_version < 1:
            from .domain import ValidationError
            raise ValidationError("expected_version必须是正整数")
        batch = self.repository.get_batch(batch_id)
        if batch["status"] == "processed":
            from .domain import ConflictError
            raise ConflictError("批次已判定，不能重复判定")
        readings = self.repository.list_readings(batch_id)
        outlet = self.repository.get_outlet_by_code(batch["outlet_code"])
        if outlet is None:
            updated = self.repository.process_batch(
                batch_id, "failed", None, None, expected_version, actor)
            self.repository.append_audit("batch_failed", "monitoring_batch", batch_id, actor, {
                "reason": "排放口不存在", "outlet_code": batch["outlet_code"],
            })
            return self._assemble_batch(updated)
        judgment = judge_batch_readings(readings, outlet["limit_value"])
        conclusion = judgment["conclusion"]
        order_no = f"OD-{batch['batch_no']}"
        updated = self.repository.process_batch(
            batch_id, "processed", conclusion, outlet["id"],
            expected_version, actor, order_no)
        self.repository.append_audit("batch_process", "monitoring_batch", batch_id, actor, {
            "conclusion": conclusion, "exceeded": len(judgment["exceeded"]),
        })
        return self._assemble_batch(updated)

    def calibrate_reading(self, reading_id: int, payload: Dict[str, Any],
                          actor: str, role: str) -> Dict[str, Any]:
        """晚到校准：校准值更新后旧超标结论失效并重算，原处置单退回复核。
        旧数据没有校准版本时补记首次入库版本。"""
        ensure_role(role, CALIBRATE_ROLES)
        actor = require_text(actor, "actor", 100)
        calibrated_value = require_number(payload.get("calibrated_value"), "calibrated_value")
        reading = self.repository.get_reading(reading_id)
        batch = self.repository.get_batch(reading["batch_id"])
        outlet = self.repository.get_outlet_by_code(batch["outlet_code"])
        if outlet is None:
            from .domain import ValidationError
            raise ValidationError("排放口不存在，无法重算")
        readings = [dict(r) for r in self.repository.list_readings(batch["id"])]
        for r in readings:
            if r["id"] == reading_id:
                r["calibrated_value"] = calibrated_value
        judgment = judge_batch_readings(readings, outlet["limit_value"])
        new_conclusion = judgment["conclusion"]
        changed = conclusion_changed(batch["conclusion"], new_conclusion)
        order_no = f"OD-{batch['batch_no']}"
        result = self.repository.apply_calibration(
            reading_id, calibrated_value, new_conclusion, changed, actor, order_no)
        self.repository.append_audit("calibration", "monitoring_batch", batch["id"], actor, {
            "reading_id": reading_id, "calibrated_value": calibrated_value,
            "new_conclusion": new_conclusion, "conclusion_changed": changed,
        })
        return result

    # ---- 处置单 ----
    def list_orders(self, role: str, status: Optional[str] = None) -> list:
        self._view(role)
        return self.repository.list_orders(status=status)

    def get_order(self, order_id: int, role: str) -> Dict[str, Any]:
        self._view(role)
        return self.repository.get_order(order_id)

    def review_order(self, order_id: int, payload: Dict[str, Any],
                     actor: str, role: str) -> Dict[str, Any]:
        """处置单复核：退回复核后结案或维持。"""
        ensure_role(role, ORDER_REVIEW_ROLES)
        actor = require_text(actor, "actor", 100)
        decision = payload.get("decision")
        if decision not in ("closed", "upheld"):
            from .domain import ValidationError
            raise ValidationError("decision必须是closed或upheld")
        new_status = "closed" if decision == "closed" else "issued"
        order = self.repository.get_order(order_id)
        updated = self.repository.update_order_status(order_id, new_status, actor)
        self.repository.append_audit("order_review", "disposal_order", order_id, actor, {
            "decision": decision, "status": new_status,
        })
        return updated

    @staticmethod
    def enrich(item: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(item)
        result["priority"] = priority_score(
            item["severity"], item["quantity"], item["threshold"])
        result["deadline_hours"] = response_deadline_hours(
            item["severity"], item["quantity"], item["threshold"])
        result["escalation_required"] = escalation_required(
            item["severity"], item["quantity"], item["threshold"])
        return result
