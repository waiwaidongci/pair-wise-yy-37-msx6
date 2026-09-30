import tempfile
import unittest
from pathlib import Path

from src.domain import ConflictError
from src.repository import Repository
from src.service import Service
from src.rules import judge_batch_readings, judge_value, effective_reading_value


class EmissionsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = Repository(str(Path(self.tmp.name) / "test.db"))
        self.service = Service(self.repo)
        self.outlet = self.service.create_outlet(
            {"code": "OUT-1", "name": "1号排放口", "pollutant": "SO2", "limit_value": 10},
            "creator", "compliance_manager")

    def tearDown(self):
        self.repo.close()
        self.tmp.cleanup()

    def _submit(self, batch_no, outlet_code="OUT-1", readings=None):
        if readings is None:
            readings = [{"pollutant": "SO2", "value": 12}]
        return self.service.submit_batch(
            {"batch_no": batch_no, "outlet_code": outlet_code, "readings": readings},
            "duty", "inspector")

    def test_outlet_crud(self):
        self.assertEqual(self.outlet["code"], "OUT-1")
        self.assertEqual(self.outlet["limit_value"], 10)
        outlets = self.service.list_outlets("viewer")
        self.assertEqual(len(outlets), 1)
        with self.assertRaises(ConflictError):
            self.service.create_outlet(
                {"code": "OUT-1", "name": "重复", "pollutant": "SO2", "limit_value": 5},
                "creator", "compliance_manager")

    def test_same_number_retransmission_uses_first_judgment(self):
        # 首次提交并判定为超标
        first = self._submit("BATCH-1")
        self.assertFalse(first["existed"])
        self.assertEqual(first["status"], "pending")
        processed = self.service.process_batch(
            first["id"], {"expected_version": 1}, "duty", "inspector")
        self.assertEqual(processed["status"], "processed")
        self.assertEqual(processed["conclusion"], "exceeded")
        self.assertEqual(len(processed["orders"]), 1)
        order = processed["orders"][0]
        self.assertEqual(order["status"], "issued")
        # 同号补传：沿用第一次判值，不重复判定、不重复开单
        replay = self._submit("BATCH-1", readings=[{"pollutant": "SO2", "value": 999}])
        self.assertTrue(replay["existed"])
        self.assertEqual(replay["conclusion"], "exceeded")
        self.assertEqual(len(replay["orders"]), 1)
        self.assertEqual(replay["orders"][0]["id"], order["id"])

    def test_failed_batch_retained_and_retried(self):
        # 排放口尚不存在 -> 失败保留
        failed = self._submit("BATCH-2", outlet_code="UNKNOWN")
        result = self.service.process_batch(
            failed["id"], {"expected_version": 1}, "duty", "inspector")
        self.assertEqual(result["status"], "failed")
        # 补建排放口后接着处理
        self.service.create_outlet(
            {"code": "UNKNOWN", "name": "补建排放口", "pollutant": "SO2", "limit_value": 10},
            "creator", "compliance_manager")
        retried = self.service.process_batch(
            failed["id"], {"expected_version": 2}, "duty", "inspector")
        self.assertEqual(retried["status"], "processed")
        self.assertEqual(retried["conclusion"], "exceeded")
        self.assertEqual(len(retried["orders"]), 1)

    def test_late_calibration_recalculates_and_returns_order_for_review(self):
        batch = self._submit("BATCH-3")
        processed = self.service.process_batch(
            batch["id"], {"expected_version": 1}, "duty", "inspector")
        order = processed["orders"][0]
        self.assertEqual(order["status"], "issued")
        reading_id = processed["readings"][0]["id"]
        # 校准值晚到：原始 12 超标，校准后 8 达标
        result = self.service.calibrate_reading(
            reading_id, {"calibrated_value": 8}, "duty", "inspector")
        self.assertTrue(result["conclusion_changed"])
        self.assertEqual(result["batch"]["conclusion"], "compliant")
        # 旧超标结论失效，原处置单退回复核
        orders = self.service.list_orders("viewer")
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["status"], "review")
        self.assertEqual(orders[0]["id"], order["id"])

    def test_calibration_backfills_first_storage_version(self):
        batch = self._submit("BATCH-4")
        self.service.process_batch(
            batch["id"], {"expected_version": 1}, "duty", "inspector")
        reading_id = batch["readings"][0]["id"]
        # 旧数据没有校准版本
        reading = self.repo.get_reading(reading_id)
        self.assertEqual(reading["calibration_version"], 0)
        self.assertEqual(self.repo.list_calibration_versions(reading_id), [])
        # 校准到达：补记首次入库版本（原始值），再记校准值
        result = self.service.calibrate_reading(
            reading_id, {"calibrated_value": 8}, "duty", "inspector")
        versions = result["versions"]
        self.assertEqual([v["version"] for v in versions], [1, 2])
        self.assertEqual(versions[0]["value"], 12)  # 首次入库原始值
        self.assertEqual(versions[1]["value"], 8)   # 校准值
        self.assertEqual(result["reading"]["calibration_version"], 2)

    def test_concurrent_submit_only_current_version_accepted(self):
        batch = self._submit("BATCH-5")
        # 值班员 A 用当前版本提交读数
        a = self.service.update_readings(
            batch["id"],
            {"readings": [{"pollutant": "SO2", "value": 14}], "expected_version": 1},
            "duty-a", "inspector")
        self.assertEqual(a["version"], 2)
        # 值班员 B 用过期版本提交 -> 收到冲突
        with self.assertRaises(ConflictError):
            self.service.update_readings(
                batch["id"],
                {"readings": [{"pollutant": "SO2", "value": 1}], "expected_version": 1},
                "duty-b", "inspector")
        # 值班员 A 先判定
        processed = self.service.process_batch(
            batch["id"], {"expected_version": 2}, "duty-a", "inspector")
        self.assertEqual(processed["status"], "processed")
        # 值班员 B 用过期版本判定 -> 收到冲突
        with self.assertRaises(ConflictError):
            self.service.process_batch(
                batch["id"], {"expected_version": 1}, "duty-b", "inspector")

    def test_review_order_closes_or_upheld(self):
        batch = self._submit("BATCH-6")
        processed = self.service.process_batch(
            batch["id"], {"expected_version": 1}, "duty", "inspector")
        order_id = processed["orders"][0]["id"]
        closed = self.service.review_order(
            order_id, {"decision": "closed"}, "reviewer", "compliance_manager")
        self.assertEqual(closed["status"], "closed")

    def test_judgment_is_pure(self):
        # 判定函数只依赖输入，无 I/O 副作用
        self.assertTrue(judge_value(11, 10))
        self.assertFalse(judge_value(9, 10))
        readings = [
            {"pollutant": "SO2", "raw_value": 12, "calibrated_value": None},
            {"pollutant": "NOx", "raw_value": 5, "calibrated_value": 5},
        ]
        result = judge_batch_readings(readings, 10)
        self.assertEqual(result["conclusion"], "exceeded")
        self.assertEqual(len(result["exceeded"]), 1)
        # 校准值优先
        calibrated = [dict(r, calibrated_value=8) for r in readings]
        result = judge_batch_readings(calibrated, 10)
        self.assertEqual(result["conclusion"], "compliant")
        self.assertEqual(effective_reading_value(
            {"raw_value": 7, "calibrated_value": None}), 7)


if __name__ == "__main__":
    unittest.main()
