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

# 排放监测判定（纯函数，与入库、请求入口分离）
BATCH_STATES=['pending','processed','failed']
ORDER_STATES=['issued','review','closed']
EXCEEDED='exceeded'; COMPLIANT='compliant'
def effective_reading_value(reading):
    """校准值优先，无校准值时取原始监测值"""
    calibrated=reading.get('calibrated_value')
    return calibrated if calibrated is not None else reading.get('raw_value')
def judge_value(value,limit):
    """单项读数判定：超过排放限值即超标"""
    return value>limit
def judge_batch_readings(readings,limit):
    """批次判定：任一读数超标则超标，否则达标"""
    exceeded=[]; compliant=[]
    for reading in readings:
        value=effective_reading_value(reading)
        (exceeded if judge_value(value,limit) else compliant).append(reading)
    conclusion=EXCEEDED if exceeded else COMPLIANT
    return {'conclusion':conclusion,'exceeded':exceeded,'compliant':compliant}
def conclusion_changed(old,new):
    return old!=new
# 角色矩阵
OUTLET_ROLES=set(['inspector','compliance_manager'])
BATCH_SUBMIT_ROLES=set(['applicant','inspector','compliance_manager'])
BATCH_PROCESS_ROLES=set(['inspector','compliance_manager'])
CALIBRATE_ROLES=set(['inspector','compliance_manager'])
ORDER_REVIEW_ROLES=set(['compliance_manager'])
