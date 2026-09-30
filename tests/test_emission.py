import tempfile, threading, unittest
from pathlib import Path
from src.domain import ConflictError, PermissionDenied, ValidationError
from src.repository import Repository
from src.service import EmissionService
from src.rules import (BATCH_DONE, BATCH_FAILED, ORDER_ISSUED, ORDER_REVIEW,
                       SOURCE_CALIBRATED, SOURCE_RAW, VERDICT_COMPLIANT,
                       VERDICT_EXCEEDANCE, calibrated_verdict, emission_verdict,
                       needs_first_ingest_backfill,
                       reupload_keeps_first_judgement, should_return_order)


class EmissionServiceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "emission.db"))
        self.svc = EmissionService(self.repo)
        self.svc.register_outlet(
            {"outlet_code": "OUT-1", "name": "一号排气筒", "limit_value": 10.0},
            "duty-a", "inspector")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _ingest(self, field_no, value, outlet="OUT-1", actor="duty-a",
                role="inspector", extra=None):
        payload = {"field_no": field_no, "measured_value": value,
                   "outlet_code": outlet}
        if extra:
            payload.update(extra)
        return self.svc.ingest_batch(payload, actor, role)

    def test_exceedance_issues_order_and_link_joins_entities(self):
        outcome = self._ingest("SCENE-1", 12.0)
        batch = outcome["batch"]
        self.assertEqual(batch["batch_status"], BATCH_DONE)
        self.assertEqual(batch["verdict"], VERDICT_EXCEEDANCE)
        self.assertEqual(batch["source"], SOURCE_RAW)
        self.assertEqual(outcome["disposal_order"]["order_status"], ORDER_ISSUED)
        link = self.svc.monitoring_link("SCENE-1", "viewer")
        self.assertEqual(link["outlet"]["outlet_code"], "OUT-1")
        self.assertEqual(link["batch"]["id"], batch["id"])
        self.assertEqual(link["orders"][0]["order_no"], "DO-SCENE-1")

    def test_reupload_keeps_first_judgement_and_order_stays_valid(self):
        first = self._ingest("SCENE-2", 11.0)
        first_version = first["batch"]["version"]
        # 补传值即使已经“达标”，也沿用第一次判值，处置单继续有效。
        again = self._ingest("SCENE-2", 5.0, extra={"expected_version": first_version})
        self.assertTrue(again["kept_first_judgement"])
        self.assertEqual(again["batch"]["verdict"], VERDICT_EXCEEDANCE)
        self.assertEqual(again["batch"]["version"], first_version)
        self.assertEqual(again["disposal_order"]["order_status"], ORDER_ISSUED)
        self.assertEqual(len(self.svc.list_orders("viewer", "SCENE-2")), 1)

    def test_concurrent_first_submit_only_one_accepted(self):
        outcomes, errors, barrier = [], [], threading.Barrier(2)

        def fire(actor, value):
            barrier.wait()
            try:
                outcomes.append(self.svc.ingest_batch(
                    {"field_no": "SCENE-C", "measured_value": value,
                     "outlet_code": "OUT-1"}, actor, "inspector"))
            except ConflictError as exc:
                errors.append(str(exc))

        t1 = threading.Thread(target=fire, args=("duty-a", 12.0))
        t2 = threading.Thread(target=fire, args=("duty-b", 5.0))
        t1.start(); t2.start(); t1.join(); t2.join()
        # 同批次同时提交：只接受一份（当前版本），后到者收到冲突，判值不被串改。
        self.assertEqual(len(outcomes), 1)
        self.assertEqual(len(errors), 1)
        accepted = outcomes[0]["batch"]
        self.assertEqual(accepted["batch_status"], BATCH_DONE)
        self.assertIn(accepted["verdict"],
                      (VERDICT_EXCEEDANCE, VERDICT_COMPLIANT))
        stored = self.svc.list_batches("viewer")
        self.assertEqual(len([b for b in stored if b["field_no"] == "SCENE-C"]), 1)
        self.assertEqual(stored[0]["verdict"], accepted["verdict"])

    def test_reupload_requires_current_version(self):
        first = self._ingest("SCENE-3", 11.0)
        # 后到者未声明版本或带旧版本：都按并发冲突拒绝。
        with self.assertRaises(ConflictError):
            self._ingest("SCENE-3", 20.0, actor="duty-b")
        with self.assertRaises(ConflictError):
            self._ingest("SCENE-3", 20.0, actor="duty-b",
                         extra={"expected_version": first["batch"]["version"] - 1})

    def test_failed_batch_retained_then_processed_after_outlet_registered(self):
        outcome = self._ingest("SCENE-4", 99.0, outlet="OUT-MISSING")
        self.assertEqual(outcome["batch"]["batch_status"], BATCH_FAILED)
        self.assertIsNone(outcome["disposal_order"])
        self.assertEqual(len(self.svc.list_batches("viewer", BATCH_FAILED)), 1)
        # 排放口补登记，接着处理失败批次：不阻断、不丢数据。
        self.svc.register_outlet(
            {"outlet_code": "OUT-MISSING", "name": "后补排口", "limit_value": 50.0},
            "duty-a", "inspector")
        retry = self.svc.retry_failed_batches("duty-a", "inspector")
        self.assertEqual(retry["retried"], 1)
        done = retry["results"][0]
        self.assertEqual(done["batch"]["batch_status"], BATCH_DONE)
        self.assertEqual(done["batch"]["verdict"], VERDICT_EXCEEDANCE)
        self.assertEqual(done["disposal_order"]["order_status"], ORDER_ISSUED)
        self.assertEqual(self.svc.list_batches("viewer", BATCH_FAILED), [])

    def test_calibration_invalidates_old_verdict_recalculates_and_returns_order(self):
        outcome = self._ingest("SCENE-5", 12.0)  # 原始：超标，发单
        self.assertEqual(outcome["disposal_order"]["order_status"], ORDER_ISSUED)
        version = outcome["batch"]["version"]
        cal = self.svc.calibrate_batch(
            {"field_no": "SCENE-5", "calibrated_value": 8.0,
             "expected_version": version}, "duty-a", "inspector")
        # 旧超标结论失效，按校准值重算为达标，原处置单退回复核。
        self.assertTrue(cal["verdict_invalidated"])
        self.assertEqual(cal["previous_verdict"], VERDICT_EXCEEDANCE)
        self.assertEqual(cal["new_verdict"], VERDICT_COMPLIANT)
        self.assertEqual(cal["batch"]["source"], SOURCE_CALIBRATED)
        self.assertEqual(cal["orders_returned"], 1)
        orders = self.svc.list_orders("viewer", "SCENE-5")
        self.assertEqual(orders[0]["order_status"], ORDER_REVIEW)
        # 旧数据没有校准版本：补记首次入库版本作为校准基线。
        self.assertTrue(cal["backfilled_first_ingest_version"])
        self.assertEqual(cal["calibration"]["basis_version"], 1)
        # 校准后同号补传仍然沿用“当前第一次判值”，不把处置单改回有效。
        again = self._ingest("SCENE-5", 12.0,
                             extra={"expected_version": cal["batch"]["version"]})
        self.assertTrue(again["kept_first_judgement"])
        self.assertEqual(
            self.svc.list_orders("viewer", "SCENE-5")[0]["order_status"],
            ORDER_REVIEW)

    def test_calibration_still_returns_order_when_remaining_exceedance(self):
        outcome = self._ingest("SCENE-6", 20.0)
        cal = self.svc.calibrate_batch(
            {"field_no": "SCENE-6", "calibrated_value": 15.0,
             "expected_version": outcome["batch"]["version"]},
            "duty-a", "inspector")
        self.assertEqual(cal["new_verdict"], VERDICT_EXCEEDANCE)
        self.assertTrue(cal["verdict_invalidated"])
        self.assertEqual(
            self.svc.list_orders("viewer", "SCENE-6")[0]["order_status"],
            ORDER_REVIEW)

    def test_compliant_first_judgement_calibration_does_not_return_order(self):
        outcome = self._ingest("SCENE-7", 5.0)
        self.assertIsNone(outcome["disposal_order"])
        cal = self.svc.calibrate_batch(
            {"field_no": "SCENE-7", "calibrated_value": 20.0,
             "expected_version": outcome["batch"]["version"]},
            "duty-a", "inspector")
        self.assertFalse(cal["verdict_invalidated"])
        self.assertEqual(cal["orders_returned"], 0)
        # 校准后超标不自动补发处置单（属于重开复核结论）。
        self.assertEqual(self.svc.list_orders("viewer", "SCENE-7"), [])

    def test_bulk_continues_past_failures_and_conflicts(self):
        report = self.svc.ingest_batches(
            {"batches": [
                {"field_no": "B-1", "measured_value": 12.0, "outlet_code": "OUT-1"},
                {"field_no": "B-2", "measured_value": 99.0, "outlet_code": "NOPE"},
                {"field_no": "B-3", "measured_value": 3.0, "outlet_code": "OUT-1"},
            ]}, "duty-a", "inspector")
        self.assertEqual(report["processed"], 3)
        self.assertEqual(report["failed_retained"], 1)
        by_no = {r["field_no"]: r for r in report["results"]}
        self.assertEqual(by_no["B-1"]["verdict"], VERDICT_EXCEEDANCE)
        self.assertEqual(by_no["B-2"]["batch_status"], BATCH_FAILED)
        self.assertEqual(by_no["B-3"]["verdict"], VERDICT_COMPLIANT)

    def test_permission_guard_for_calibration(self):
        self._ingest("SCENE-8", 12.0)
        with self.assertRaises(PermissionDenied):
            self.svc.calibrate_batch(
                {"field_no": "SCENE-8", "calibrated_value": 1.0,
                 "expected_version": 2}, "duty-c", "viewer")

    def test_audit_chain_intact(self):
        self._ingest("SCENE-9", 12.0)
        self.svc.retry_failed_batches("duty-a", "inspector")
        self.assertTrue(self.repo.verify_audit_chain())


class EmissionRulesTest(unittest.TestCase):
    def test_verdict_threshold_is_strict(self):
        self.assertEqual(emission_verdict(10.0, 10.0), VERDICT_COMPLIANT)
        self.assertEqual(emission_verdict(10.0001, 10.0), VERDICT_EXCEEDANCE)
        self.assertEqual(calibrated_verdict(2.0, 3.0), VERDICT_COMPLIANT)

    def test_reupload_keeps_first_judgement(self):
        self.assertEqual(
            reupload_keeps_first_judgement(VERDICT_EXCEEDANCE), VERDICT_EXCEEDANCE)

    def test_return_order_only_when_prior_was_exceedance(self):
        self.assertTrue(should_return_order(VERDICT_EXCEEDANCE))
        self.assertFalse(should_return_order(VERDICT_COMPLIANT))

    def test_backfill_flag_for_legacy_data(self):
        self.assertTrue(needs_first_ingest_backfill(None))
        self.assertFalse(needs_first_ingest_backfill(1))
        with self.assertRaises(ValidationError):
            reupload_keeps_first_judgement("unknown")


if __name__ == "__main__":
    unittest.main()
