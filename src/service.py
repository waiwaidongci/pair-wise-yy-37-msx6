from __future__ import annotations

from typing import Any, Dict, Optional

from .domain import (ConflictError, DomainError, NotFoundError,
                     ValidationError, ensure_role, normalize_severity,
                     require_int, require_number, require_text)
from .repository import Repository
from .rules import (AUDIT_ROLES, CREATE_ROLES, EM_CALIBRATE_ROLES, EM_ENTITY,
                    EM_INGEST_ROLES, EM_RETRY_ROLES, ENTITY, RECORD_ROLES,
                    VIEW_ROLES, calibrated_verdict, completion_blockers,
                    escalation_required, needs_first_ingest_backfill,
                    priority_score, response_deadline_hours, role_for_transition,
                    should_return_order, validate_transition)


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


class EmissionService:
    """监测批次、排放口与处置单的用例编排。

    判定规则全部来自 ``rules``；状态与版本落库由 ``repository`` 承担；
    本类只做权限、入参校验与事务顺序编排。
    """

    def __init__(self, repository: Repository):
        self.repository = repository

    # -- 排放口 ----------------------------------------------------------
    def register_outlet(self, payload: Dict[str, Any], actor: str,
                        role: str) -> Dict[str, Any]:
        ensure_role(role, EM_INGEST_ROLES)
        actor = require_text(actor, "actor", 100)
        outlet_code = require_text(payload.get("outlet_code"), "outlet_code", 100)
        name = require_text(payload.get("name"), "name", 200)
        limit_value = require_number(payload.get("limit_value"), "limit_value",
                                     0.000001)
        outlet = self.repository.create_outlet(outlet_code, name, limit_value, actor)
        self.repository.append_audit("outlet_register", EM_ENTITY, outlet["id"], actor, {
            "outlet_code": outlet_code, "limit_value": limit_value,
        })
        return outlet

    def list_outlets(self, role: str) -> list:
        ensure_role(role, VIEW_ROLES)
        return self.repository.list_outlets()

    # -- 监测批次入库与判值 ----------------------------------------------
    def ingest_batch(self, payload: Dict[str, Any], actor: str,
                     role: str) -> Dict[str, Any]:
        ensure_role(role, EM_INGEST_ROLES)
        actor = require_text(actor, "actor", 100)
        field_no = require_text(payload.get("field_no"), "field_no", 100)
        measured_value = require_number(payload.get("measured_value"),
                                        "measured_value", 0.0)
        outlet_raw = payload.get("outlet_code")
        outlet_code = (require_text(outlet_raw, "outlet_code", 100)
                       if outlet_raw is not None else None)
        expected = payload.get("expected_version")
        if expected is not None:
            expected = require_int(expected, "expected_version")
        outlet = (self.repository.get_outlet_by_code(outlet_code)
                  if outlet_code else None)
        limit_value = outlet["limit_value"] if outlet else None

        def audit_plan(entity_id: int) -> list:
            batch = self.repository.get_batch_by_id(entity_id)
            state = outcome["state"]
            if state == "failed":
                return [{"action": "batch_failed", "detail": {
                    "field_no": field_no,
                    "reason": batch["failure_reason"]}}]
            if state == "waiting":
                return [{"action": "batch_retry_waiting",
                         "detail": {"field_no": field_no}}]
            if state == "reuploaded":
                return [{"action": "batch_reupload", "detail": {
                    "field_no": field_no,
                    "first_verdict": batch["verdict"],
                    "kept_first_judgement": True}}]
            order = outcome.get("order")
            detail = {"field_no": field_no, "verdict": batch["verdict"],
                      "measured_value": measured_value,
                      "limit_value": limit_value,
                      "disposal_order_id": order["id"] if order else None}
            action = "batch_ingest" if state == "created" else "batch_retry"
            if action == "batch_retry":
                detail["reupload"] = True
            return [{"action": action, "detail": detail}]

        outcome = {"state": None, "order": None}
        # 裁决、入库、判值、发单与审计全部在仓储的同一持锁事务里完成。
        tx = self.repository.ingest_batch_tx(
            field_no, outlet_code, measured_value, limit_value, expected, actor,
            audit=audit_plan, outcome=outcome)
        return self._present(tx, outlet)

    def _present(self, tx: Dict[str, Any], outlet) -> Dict[str, Any]:
        outcome = tx["outcome"]
        batch = tx["batch"]
        if outcome == "created" and outlet is None:
            return {"batch": batch, "outlet": None, "disposal_order": None,
                    "reupload": False, "kept_first_judgement": False}
        if outcome == "created":
            return {"batch": batch, "outlet": outlet, "disposal_order": tx["order"],
                    "reupload": False, "kept_first_judgement": False}
        if outcome == "waiting":
            return {"batch": batch, "outlet": None, "disposal_order": None,
                    "reupload": True, "kept_first_judgement": False}
        if outcome == "resumed":
            return {"batch": batch, "outlet": outlet, "disposal_order": tx["order"],
                    "reupload": True, "kept_first_judgement": False}
        # reuploaded：同号补传沿用第一次判值，已发出处置单继续有效。
        return {"batch": batch, "outlet": self._outlet_of(batch),
                "disposal_order": tx["order"], "reupload": True,
                "kept_first_judgement": True}

    def ingest_batches(self, payload: Dict[str, Any], actor: str,
                       role: str) -> Dict[str, Any]:
        ensure_role(role, EM_INGEST_ROLES)
        batches = payload.get("batches")
        if not isinstance(batches, list) or not batches:
            raise ValidationError("batches必须是非空数组")
        results = []
        for index, item in enumerate(batches):
            if not isinstance(item, dict):
                results.append({"index": index, "ok": False,
                                "error": "批次必须是JSON对象"})
                continue
            field_no = item.get("field_no")
            try:
                outcome = self.ingest_batch(item, actor, role)
                results.append({
                    "index": index, "field_no": field_no, "ok": True,
                    "batch_status": outcome["batch"]["batch_status"],
                    "verdict": outcome["batch"]["verdict"],
                    "disposal_order_id": (outcome["disposal_order"] or {}).get("id"),
                    "kept_first_judgement": outcome["kept_first_judgement"],
                })
            except ConflictError as exc:
                # 同批次并发提交：只接受当前版本，后到者收到冲突，其它批次继续处理。
                results.append({"index": index, "field_no": field_no, "ok": False,
                                "conflict": True, "message": str(exc)})
            except DomainError as exc:
                results.append({"index": index, "field_no": field_no, "ok": False,
                                "message": str(exc)})
        failed_retained = sum(
            1 for r in results if r.get("batch_status") == "failed")
        return {"processed": len(results), "failed_retained": failed_retained,
                "results": results}

    # -- 失败批次接着处理 ------------------------------------------------
    def retry_failed_batches(self, actor: str, role: str) -> Dict[str, Any]:
        ensure_role(role, EM_RETRY_ROLES)
        actor = require_text(actor, "actor", 100)
        results = []
        for failed in self.repository.list_failed_batches():
            results.append(self._retry_one(failed, actor, reupload=False))
        return {"retried": len(results),
                "results": [r for r in results]}

    def _retry_one(self, failed: Dict[str, Any], actor: str,
                   reupload: bool) -> Dict[str, Any]:
        field_no = failed["field_no"]
        outlet = self._outlet_of(failed)
        if outlet is None:
            pending = self.repository.touch_failed_batch_tx(
                failed["id"], "排放口仍未登记", actor,
                audit=[{"action": "batch_retry_waiting",
                        "detail": {"field_no": field_no}}])
            return {"batch": pending, "outlet": None, "disposal_order": None,
                    "reupload": reupload, "kept_first_judgement": False}
        result = {}

        def audit_plan(entity_id: int) -> list:
            return [{"action": "batch_retry", "detail": {
                "field_no": field_no, "verdict": result["verdict"],
                "reupload": reupload,
                "disposal_order_id": result["order"]["id"]
                    if result["order"] else None}}]

        tx = self.repository.retry_batch_tx(
            failed["id"], outlet["limit_value"], actor,
            audit=audit_plan, outcome=result)
        return {"batch": tx["batch"], "outlet": outlet, "disposal_order": tx["order"],
                "reupload": reupload, "kept_first_judgement": False}

    # -- 校准晚到：旧结论失效、重算、处置单退回 ---------------------------
    def calibrate_batch(self, payload: Dict[str, Any], actor: str,
                        role: str) -> Dict[str, Any]:
        ensure_role(role, EM_CALIBRATE_ROLES)
        actor = require_text(actor, "actor", 100)
        field_no = require_text(payload.get("field_no"), "field_no", 100)
        calibrated_value = require_number(payload.get("calibrated_value"),
                                          "calibrated_value", 0.0)
        expected_version = require_int(payload.get("expected_version"),
                                       "expected_version")
        batch = self.repository.require_batch_by_field_no(field_no)
        if batch["version"] != expected_version:
            raise ConflictError("版本冲突，该批次已被更新，请刷新后重试")
        if batch["batch_status"] != "done":
            raise ConflictError("批次尚未完成首次判值，无法校准")
        outlet = self._outlet_of(batch)
        if outlet is None:
            raise NotFoundError("批次缺少排放口，无法按限值重算")
        previous_verdict = batch["verdict"]
        # 旧数据没有校准版本时，补记首次入库版本作为校准基线。
        backfill = needs_first_ingest_backfill(batch["calibration_version"])
        basis_version = (batch["first_ingest_version"] if backfill
                         else batch["calibration_version"])
        new_verdict = calibrated_verdict(calibrated_value, outlet["limit_value"])
        returned = should_return_order(previous_verdict)
        backfill = needs_first_ingest_backfill(batch["calibration_version"])
        # orders_returned 由仓储在事务内实际退回处置单后回填。
        audit_detail = {
            "field_no": field_no,
            "previous_verdict": previous_verdict,
            "new_verdict": new_verdict,
            "basis_version": basis_version,
            "backfilled_first_ingest_version": backfill,
            "orders_returned": 0,
        }
        audit_spec = [{"action": "batch_calibrate", "detail": audit_detail}]
        updated = self.repository.apply_calibration(
            field_no, calibrated_value, basis_version, previous_verdict,
            new_verdict, expected_version, actor, audit=audit_spec)
        calibration = updated.pop("_calibration")
        orders_returned = updated.pop("_orders_returned", 0)
        if not returned:
            orders_returned = 0
        return {
            "batch": updated,
            "calibration": calibration,
            "outlet": outlet,
            "previous_verdict": previous_verdict,
            "new_verdict": new_verdict,
            "verdict_invalidated": returned,
            "orders_returned": orders_returned,
            "backfilled_first_ingest_version": backfill,
        }

    # -- 查询 ------------------------------------------------------------
    def list_batches(self, role: str, status: Optional[str] = None) -> list:
        ensure_role(role, VIEW_ROLES)
        return self.repository.list_batches(status)

    def list_orders(self, role: str, field_no: Optional[str] = None) -> list:
        ensure_role(role, VIEW_ROLES)
        return self.repository.list_orders(field_no)

    def monitoring_link(self, field_no: str, role: str) -> Dict[str, Any]:
        ensure_role(role, VIEW_ROLES)
        field_no = require_text(field_no, "field_no", 100)
        return self.repository.link_by_field_no(field_no)

    # -- 内部辅助 --------------------------------------------------------
    def _outlet_of(self, batch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        outlet_code = batch.get("outlet_code")
        if not outlet_code:
            return None
        return self.repository.get_outlet_by_code(outlet_code)
