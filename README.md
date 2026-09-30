# 空气污染源许可与合规检查

管理设施、排放口、治理设备、现场检查、整改和许可续期。

## 模块结构

- `app.py`：参数解析、依赖组装和HTTP服务启动。
- `src/domain.py`：数据结构、错误、状态和基础校验。
- `src/rules.py`：状态机、角色矩阵、优先级、期限和关闭不变量。
- `src/repository.py`：SQLite建表、事务、版本控制和审计链。
- `src/service.py`：权限检查、用例编排、并发控制和审计。
- `src/http_api.py`：JSON路由和统一错误响应。
- `src/audit.py`：UTC时间和SHA-256审计事件。
- `static/index.html`：最小演示页。
- `tests/`：完整流程、规则和失败测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8314
```

默认端口为`8314`，首次启动自动建库。使用`X-Actor`和`X-Role`请求头传递身份。

## 主要接口

- `GET /health`
- `GET /api/items`
- `POST /api/items`
- `GET /api/items/{id}`
- `POST /api/items/{id}/records`
- `POST /api/items/{id}/transition`，必须提交`expected_version`
- `GET /api/audit`

允许角色：applicant, inspector, compliance_manager, viewer。申报量超过许可量或检查发现高严重度问题时提高优先级；存在未关闭整改时不能批准。

## 排放监测、校准晚到与超标处置单

监测批次、排放口和处置单通过现场单号（`field_no`）串联：

- 首次入库即判值：测量值严格大于排放口限值判超标（`exceedance`），超标时自动发出处置单（`issued`）。
- 同号补传沿用第一次判值：不重算、不重复发单，已发出的处置单继续有效。补传需携带当前 `expected_version`。
- 排放口未登记的批次判为失败并**保留**（`failed`），不阻断其它批次；排放口补登记后可调用接续接口继续处理。
- 校准值晚到：旧超标结论失效并按校准值重算；先前判超标的，原处置单退回**复核**（`review`）。旧数据没有校准版本时，补记首次入库版本（`first_ingest_version`）作为校准基线。
- 两名值班员同时提交同一批次时，只接受当前版本，后到者（无版本号或版本号过期）收到 `409` 冲突。判定/入库/审计在仓储的同一持锁事务内原子完成。

接口（角色沿用 `X-Role`，applicant 与 inspector 可上报，仅 inspector 可校准）：

- `POST /api/emission/outlets`：登记排放口（`outlet_code`、`name`、`limit_value`）。
- `POST /api/emission/batches`：单批入库，支持可选 `outlet_code` 与 `expected_version`。
- `POST /api/emission/batches/bulk`：批量入库，失败/冲突逐条返回，不相互阻断。
- `POST /api/emission/batches/retry-failed`：接续处理所有保留的失败批次。
- `POST /api/emission/calibrations`：按校准值重算（必带 `expected_version`）。
- `GET /api/emission/batches?status=failed`、`GET /api/emission/orders?field_no=...`。
- `GET /api/emission/links/{field_no}`：按现场单号连起排放口、批次、校准记录与处置单。

## 测试

```bash
python3 -m unittest discover -s tests -v
```
