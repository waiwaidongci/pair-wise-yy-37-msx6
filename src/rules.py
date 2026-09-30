from __future__ import annotations
from .domain import ConflictError, ValidationError
TITLE='空气污染源许可与合规检查'; ENTITY='排污许可'; ID_PREFIX='AQ'
SEVERITIES=['low', 'medium', 'high', 'critical']; STATES=['draft', 'submitted', 'inspection', 'correction', 'approved']; TRANSITIONS={'draft': ['submitted'], 'submitted': ['inspection'], 'inspection': ['correction'], 'correction': ['approved'], 'approved': []}; TRANSITION_ROLES={'submitted': ['applicant'], 'inspection': ['inspector'], 'correction': ['inspector'], 'approved': ['compliance_manager']}
CREATE_ROLES=set(['applicant']); RECORD_ROLES=set(['applicant', 'inspector']); AUDIT_ROLES=set(['compliance_manager', 'viewer']); VIEW_ROLES=set(['applicant', 'inspector', 'compliance_manager', 'viewer'])
SEVERITY_WEIGHT={'low': 1.0, 'medium': 3.0, 'high': 6.0, 'critical': 9.0}; DEADLINE_HOURS={'low': 72, 'medium': 24, 'high': 8, 'critical': 4}; TERMINAL_STATES=set(['approved'])
def priority_score(severity,quantity=0.0,threshold=1.0,open_records=0):
    if severity not in SEVERITY_WEIGHT: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(0,min(10,int(round(SEVERITY_WEIGHT[severity]+min(4.0,ratio*4.0)+min(3.0,float(open_records))))))
def response_deadline_hours(severity,quantity=0.0,threshold=1.0):
    if severity not in DEADLINE_HOURS: raise ValidationError("unknown severity")
    ratio=quantity/threshold if threshold>0 else 1.0
    return max(1,int(DEADLINE_HOURS[severity]/max(1.0,ratio)))
def escalation_required(severity,quantity=0.0,threshold=1.0):
    return severity==SEVERITIES[-1] or (threshold>0 and quantity>=threshold)
def can_transition(current,target): return target in TRANSITIONS.get(current,[])
def validate_transition(current,target):
    if current not in STATES or target not in STATES: raise ValidationError("未知状态")
    if not can_transition(current,target): raise ConflictError(f"不能从{current}转换到{target}")
def completion_blockers(target,open_records): return ["仍有未关闭事项"] if target in TERMINAL_STATES and open_records>0 else []
def role_for_transition(target): return set(TRANSITION_ROLES.get(target,[]))

# ---------------------------------------------------------------------------
# 排放监测：批次判值、校准重算、处置单复核
# ---------------------------------------------------------------------------
EM_ENTITY='排放监测'
VERDICT_COMPLIANT='compliant'; VERDICT_EXCEEDANCE='exceedance'
EM_VERDICTS=[VERDICT_COMPLIANT, VERDICT_EXCEEDANCE]
# 批次：待处理 / 处理完成 / 处理失败（失败保留，待出口补齐后续处理）
BATCH_PENDING='pending'; BATCH_DONE='done'; BATCH_FAILED='failed'
EM_BATCH_STATES=[BATCH_PENDING, BATCH_DONE, BATCH_FAILED]
# 处置单：已发出（继续有效）/ 退回复核（旧超标结论已失效）
ORDER_ISSUED='issued'; ORDER_REVIEW='review'
EM_ORDER_STATES=[ORDER_ISSUED, ORDER_REVIEW]
# 数据来源：原始上报 / 校准补传（校准版本）
SOURCE_RAW='raw'; SOURCE_CALIBRATED='calibrated'
EM_SOURCES=[SOURCE_RAW, SOURCE_CALIBRATED]
EM_INGEST_ROLES=set(['applicant','inspector'])
EM_CALIBRATE_ROLES=set(['inspector'])
EM_RETRY_ROLES=set(['applicant','inspector'])

def emission_verdict(measured,limit_value):
    """严格大于限值判超标；等于限值仍为达标。"""
    if measured>limit_value: return VERDICT_EXCEEDANCE
    return VERDICT_COMPLIANT

def calibrated_verdict(calibrated_value,limit_value):
    """校准值更新后的判值规则与首次判值一致。"""
    return emission_verdict(calibrated_value,limit_value)

def should_issue_order(verdict):
    return verdict==VERDICT_EXCEEDANCE

def should_return_order(prior_verdict):
    """校准值更新后旧超标结论失效：先前判超标则原处置单退回复核。"""
    return prior_verdict==VERDICT_EXCEEDANCE

def reupload_keeps_first_judgement(first_verdict):
    """同号补传沿用第一次判值：补传不重新判值，返回首次判值。"""
    if first_verdict not in EM_VERDICTS: raise ValidationError("未知判值")
    return first_verdict

def needs_first_ingest_backfill(calibration_version):
    """旧数据没有校准版本时，补记首次入库版本作为校准基线。"""
    return calibration_version is None

def next_batch_status(ok):
    return BATCH_DONE if ok else BATCH_FAILED
