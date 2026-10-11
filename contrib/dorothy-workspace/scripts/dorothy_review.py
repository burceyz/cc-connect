#!/usr/bin/env python3
"""Fail-closed Dorothy publication gate; model execution uses the existing DSH jobs bridge."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import bridge
import instances
import ipc
import jobs


PROTOCOL = 'cc-connect-review-gate-v1'
MANIFEST_SCHEMA = 'cc-connect-dorothy-manifest-v1'
REVIEW_SCHEMA = 'cc-connect-hermes-review-v1'
RESPONSE_SCHEMA = 'cc-connect-dorothy-response-v1'
REVIEW_ROOT = bridge.WORKSPACE / '.local/dorothy-reviews'
MAX_INPUT = 4 * 1024 * 1024
MAX_FILE = 8 * 1024 * 1024
MAX_MATERIALS = 32 * 1024 * 1024
TIMEOUT = 900
REFRESH_TIMEOUT = 55
PROFILE = 'opencode-go'
EFFORT = 'high'
RUNTIME_EVIDENCE_FIELDS = {'configured_model', 'observed_model', 'effort', 'mode'}
HEX = re.compile(r'[0-9a-f]{64}')
SAFE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}')
CHILD_ID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode('utf-8')


def read_bytes(path, limit=MAX_FILE):
    if path.is_symlink() or not path.is_file():
        raise ValueError('file_missing_or_symlink')
    with path.open('rb') as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise ValueError('file_too_large')
    return data


def read_json(path):
    value = json.loads(read_bytes(path))
    if not isinstance(value, dict):
        raise ValueError('invalid_json_object')
    return value


def atomic_json(path, value):
    fd, name = tempfile.mkstemp(prefix='.gate-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as handle:
            handle.write(json_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def immutable(path, data):
    if path.exists() or path.is_symlink():
        if read_bytes(path, MAX_MATERIALS) != data:
            raise ValueError('frozen_material_modified')
        return
    with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o400), 'wb') as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def text_field(value, code='invalid_text'):
    if not isinstance(value, str) or not value.strip() or '\x00' in value:
        raise ValueError(code)
    return value


def strings(value, nonempty=False):
    if not isinstance(value, list) or (nonempty and not value):
        raise ValueError('invalid_string_list')
    for item in value:
        text_field(item)
    return value


def valid_hash(value):
    return isinstance(value, str) and HEX.fullmatch(value) is not None


def validate_origin(value):
    if not isinstance(value, dict) or set(value) != {'project', 'session_key', 'cwd'}:
        raise ValueError('invalid_origin')
    for item in value.values():
        text_field(item, 'invalid_origin')
    if value['cwd'] != str(bridge.WORKSPACE.resolve()):
        raise ValueError('origin_workspace_mismatch')
    return value


def runtime_evidence(value):
    if not isinstance(value, dict) or not set(value).issubset(RUNTIME_EVIDENCE_FIELDS):
        raise ValueError('runtime_evidence_invalid')
    if any(not isinstance(item, str) or not item.strip() or len(item) > 128 or
           any(ord(char) < 32 or ord(char) == 127 for char in item) for item in value.values()):
        raise ValueError('runtime_evidence_invalid')
    return dict(value)


@contextmanager
def refresh_deadline():
    """The CLI refresh is bounded even when multiple native sources need decoding."""
    def expired(signum, frame):
        raise ValueError('review_refresh_timeout')
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    started = time.monotonic()
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, min(REFRESH_TIMEOUT, previous_timer[0] or REFRESH_TIMEOUT))
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0]:
            signal.setitimer(signal.ITIMER_REAL, max(0.001, previous_timer[0] - (time.monotonic() - started)),
                             previous_timer[1])


def capacity_reason(runtime, cwd):
    """Observe jobs, running records and leases; jobs still owns the atomic execution leases."""
    control = runtime / 'coordination'
    occupied = 0
    for index in range(instances.CAPACITY):
        path = control / ('capacity-' + str(index) + '.lock')
        if path.exists():
            state = jobs.lock_probe(path)
            if state == 'unavailable':
                return 'capacity_unconfirmed'
            occupied += state == 'held'
    reserved = 0
    for path in (runtime / 'jobs').glob('*/receipt.json'):
        record = read_json(path)
        if record.get('status') in jobs.ACTIVE and not record.get('run_dir'):
            state = jobs.lock_probe(path.parent / 'worker.lock')
            if state == 'held':
                reserved += 1
            elif state == 'unavailable':
                return 'job_occupancy_unconfirmed'
    # Runs include synchronous calls and offline preflight, which have no job receipt.
    running = 0
    for path in (runtime / 'runs').glob('*/result.json'):
        record = read_json(path)
        if record.get('status') != 'running':
            continue
        instance = instances.root(runtime, record.get('instance_id'))
        state = jobs.lock_probe(instance / 'bridge.lock')
        if state == 'held':
            running += 1
        elif state == 'unavailable':
            return 'run_occupancy_unconfirmed'
    if max(occupied, running) + reserved >= instances.CAPACITY:
        return 'capacity_busy'
    for path in (control / 'workspaces').glob('*.lock'):
        state = jobs.lock_probe(path)
        if state == 'unavailable':
            return 'workspace_occupancy_unconfirmed'
        if state == 'held':
            other = Path(text_field(read_json(path).get('cwd')))
            if not other.is_absolute():
                return 'workspace_occupancy_unconfirmed'
            if cwd.is_relative_to(other) or other.is_relative_to(cwd):
                return 'workspace_busy'
    return None


def native_config(rows):
    configs = [row['data']['header']['config'] for row in rows if row.get('type') == 'request/header']
    expected = (bridge.PROFILE_PROVIDERS[PROFILE], 'deepseek-v4.1-flash', EFFORT)
    if not configs or any((c.get('provider'), c.get('model'), c.get('reasoningEffort')) != expected for c in configs):
        raise ValueError('native_review_model_mismatch')
    return len(configs)


def native_child_rows(child_id, runtime, instance_id=None):
    """Read only a catalog-selected UUID child in the parent's existing instance."""
    if not isinstance(child_id, str) or not CHILD_ID.fullmatch(child_id):
        raise ValueError('native_child_id_invalid')
    home = instances.root(runtime, instance_id) / 'home'
    matches = list((home / 'sessions').glob('*/' + child_id + '/session.v4.jsonl.zstd'))
    if len(matches) != 1 or not shutil.which('zstd'):
        raise ValueError('native_child_source_unavailable')
    source = matches[0]
    if source.is_symlink() or not source.resolve().is_relative_to(home.resolve()):
        raise ValueError('native_child_source_outside_runtime')
    raw = subprocess.run(['zstd', '-dc', str(source)], stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, check=True, timeout=30).stdout
    return source, [json.loads(line) for line in raw.splitlines() if line.strip()]


def native_window(rows, run, task_text, final_text):
    """Use the exporter's visible-message boundary, excluding subsequent turns and reasoning."""
    if not rows or rows[0].get('id') != run.get('session_id') or rows[0].get('cwd') != run['cwd']:
        raise ValueError('native_session_mismatch')
    before = run.get('source_before_seq')
    if type(before) is not int:
        raise ValueError('native_source_cursor_missing')
    messages = bridge.visible_messages(rows)
    starts = [i for i, message in enumerate(messages) if message['seq'] > before and
              message['speaker'] == 'Codex' and message['text'].strip() == task_text.strip()]
    if not starts:
        raise ValueError('native_task_not_in_source')
    start = starts[0]
    end = next((i for i in range(start + 1, len(messages)) if messages[i]['speaker'] == 'Codex'), len(messages))
    answers = messages[start + 1:end]
    if not answers:
        raise ValueError('native_final_not_in_source')
    last = answers[-1]['seq']
    final = ''.join(message['text'] for message in answers if message['seq'] == last)
    if final != final_text:
        raise ValueError('native_final_not_in_source')
    first = messages[start]['seq']
    return [row for row in rows if type(row.get('seq')) is int and first <= row['seq'] <= last], first, last


def verify_native(run, task_text, final_text, runtime, materials, revision_id):
    """Verify the actual parent turn, synchronous subagent results and direct-child model routes.

    Schema follows the installed DSH tool-subagent/session packages, not report claims.
    Only identities, sequence numbers, hashes and route metadata are returned/persisted.
    """
    source, rows = bridge.session_rows(run['session_id'], runtime, run.get('instance_id'))
    window, first, last = native_window(rows, run, task_text, final_text)
    request_count = native_config(window)
    calls = [row for row in window if row.get('type') == 'tool/call' and row['data'].get('name') == 'subagent']
    successes = []
    child_ids, call_ids, descriptions = set(), set(), set()
    for call in calls:
        data = call['data']
        call_id = text_field(data.get('callId'), 'native_subagent_call_missing')
        args = json.loads(data['arguments'])
        if args.get('run_in_background') is not False:
            raise ValueError('native_subagent_not_synchronous')
        description = text_field(args.get('description'), 'native_subagent_description_missing')
        prompt = text_field(args.get('prompt'), 'native_subagent_materials_missing')
        if str(materials) not in prompt or revision_id not in prompt:
            raise ValueError('native_subagent_materials_mismatch')
        results = [row for row in window if row.get('type') == 'tool/result' and
                   row['data'].get('message', {}).get('toolCallId') == call_id]
        if len(results) != 1 or results[0]['seq'] <= call['seq']:
            raise ValueError('native_subagent_result_missing')
        result = results[0]['data']
        message = result.get('message', {})
        if result.get('error') or message.get('isError') is not False:
            raise ValueError('native_subagent_failed')
        result_text = ''.join(block.get('text', '') for block in message.get('content', []) if block.get('type') == 'text')
        text_field(result_text, 'native_subagent_result_empty')
        # Foreground success is emitted only after completed + disposal by the DSH tool.
        # Parent-owned catalog labels originate from the same invocation description.
        children = [row['data'] for row in window if row.get('type') == 'subagent/catalog' and
                    row['data'].get('label') == description and call['seq'] < row['seq'] < results[0]['seq']]
        if len(children) != 1 or children[0].get('mode') != 'one-shot':
            raise ValueError('native_child_catalog_mismatch')
        child_id = text_field(children[0].get('childId'), 'native_child_id_missing')
        if child_id in child_ids or call_id in call_ids or description in descriptions:
            raise ValueError('native_duplicate_subreview')
        child_source, child_rows = native_child_rows(child_id, runtime, run.get('instance_id'))
        if (not child_rows or child_rows[0].get('id') != child_id or
                child_rows[0].get('parentSession') != run['session_id'] or
                child_rows[0].get('cwd') != str(materials)):
            raise ValueError('native_child_session_mismatch')
        child_requests = native_config(child_rows)
        child_messages = [row for row in child_rows if row.get('type') == 'assistant/message']
        if not child_messages:
            raise ValueError('native_child_final_missing')
        child_final = ''.join(block.get('text', '') for block in child_messages[-1]['data']['message'].get('content', [])
                              if block.get('type') == 'text')
        text_field(child_final, 'native_child_final_missing')
        if child_final != result_text:
            raise ValueError('native_child_result_mismatch')
        successes.append({'call_id': call_id, 'description': description, 'call_seq': call['seq'],
                          'result_seq': results[0]['seq'], 'child_session_id': child_id,
                          'child_request_count': child_requests, 'child_source_sha256': sha(child_source.read_bytes()),
                          'child_final_sha256': sha(child_final.encode()), 'tool_result_sha256': sha(result_text.encode())})
        child_ids.add(child_id)
        call_ids.add(call_id)
        descriptions.add(description)
    if len(successes) < 2:
        raise ValueError('native_two_subreviews_missing')
    return {'session_id': run['session_id'], 'cwd': run['cwd'], 'source_sha256': sha(source.read_bytes()),
            'first_seq': first, 'last_seq': last, 'request_count': request_count,
            'provider': bridge.PROFILE_PROVIDERS[PROFILE], 'model': 'deepseek-v4.1-flash',
            'reasoning_effort': EFFORT, 'subreviews': successes}


class Gate:
    def __init__(self, root=REVIEW_ROOT, runtime=bridge.RUNTIME):
        self.root = Path(root).absolute()
        self.runtime = Path(runtime).resolve()
        self.reply = {}

    @contextmanager
    def locked(self, task_id=None):
        if self.root.is_symlink():
            raise ValueError('review_root_symlink')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name in ('tasks', 'events', 'conversations'):
            path = self.root / name
            if path.is_symlink():
                raise ValueError('review_root_symlink')
            path.mkdir(exist_ok=True, mode=0o700)
        directory = self.task_dir(task_id) if task_id else self.root
        fd = os.open(directory / 'gate.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'r+') as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError('review_gate_busy') from None
            yield

    def task_dir(self, task_id):
        if not isinstance(task_id, str) or not SAFE_ID.fullmatch(task_id):
            raise ValueError('invalid_task_id')
        path = self.root / 'tasks' / task_id
        if path.is_symlink() or not path.resolve().is_relative_to((self.root / 'tasks').resolve()):
            raise ValueError('task_path_invalid')
        return path

    def outcome(self, status, *reasons, **extra):
        return self.reply | {'status': status, 'allow_publish': status == 'verified',
                             'reasons': list(reasons)} | extra

    def conversation_path(self, origin, conversation):
        text_field(conversation, 'conversation_id_missing')
        key = sha(json_bytes({'origin': origin, 'conversation_id': conversation}))
        return self.root / 'conversations' / (key + '.json')

    def task_context(self, payload):
        dest = self.task_dir(payload['task_id'])
        task = read_json(dest / 'task.json')
        if task['task_id'] != payload['task_id'] or task['origin'] != payload['origin']:
            raise ValueError('task_origin_mismatch')
        if sha(read_bytes(dest / 'request.txt')) != task['request_sha256']:
            raise ValueError('original_request_modified')
        self.describe_task(dest, task)
        return dest, task

    def describe_task(self, dest, task):
        self.reply.update(task_dir=str(dest), manifest_path=str(dest / 'manifest.json'))
        self.reply['instructions'] = self.instructions(dest, task)
        if task.get('active_revision'):
            revision = read_json(dest / 'revisions' / task['active_revision'] / 'revision.json')
            self.reply.update(revision=revision['revision'], candidate_sha256=revision['candidate_sha256'],
                              manifest_sha256=revision['manifest_sha256'], hermes_job_id=revision['job_id'])

    def index_conversation(self, task, status=None):
        # The order is allocated at begin, never at callback time. A delayed first
        # bind or legacy callback therefore cannot displace a newer user task.
        with self.locked():
            path = self.conversation_path(task['origin'], task['conversation_id'])
            if path.exists():
                current = read_json(path)
                if current.get('task_id') == task['task_id'] and status is None:
                    return
                if current.get('task_id') != task['task_id']:
                    if status == 'verified':
                        return
                    previous = read_json(self.task_dir(current.get('task_id')) / 'task.json')
                    if (previous.get('origin') != task['origin'] or
                            previous.get('conversation_id') != task['conversation_id']):
                        raise ValueError('conversation_index_mismatch')
                    if task.get('creation_order', 0) <= previous.get('creation_order', 0):
                        return
            elif status == 'verified':
                return
            atomic_json(path, {'task_id': task['task_id'], 'status': status or 'pending_review'})

    def handle(self, payload):
        self.reply = {'protocol': PROTOCOL, 'task_id': '', 'candidate_sha256': '',
                      'manifest_sha256': '', 'hermes_job_id': None, 'evidence_refs': [],
                      'instructions': ''}
        try:
            if not isinstance(payload, dict) or payload.get('protocol') != PROTOCOL:
                raise ValueError('protocol_mismatch')
            self.task_dir(payload.get('task_id'))
            self.reply['task_id'] = payload['task_id']
            validate_origin(payload.get('origin'))
            if payload.get('op') == 'begin':
                # Only local identity and frozen state are read during begin.
                # Report retrieval is an explicit, bounded per-task operation.
                with self.locked():
                    return self.begin(payload)
            with self.locked(payload['task_id']):
                if payload.get('op') == 'bind':
                    return self.bind(payload)
                if payload.get('op') == 'refresh':
                    with refresh_deadline():
                        return self.refresh(payload)
                if payload.get('op') == 'check':
                    return self.check(payload)
                if payload.get('op') == 'finalize':
                    return self.finalize(payload)
                raise ValueError('invalid_operation')
        except Exception as exc:
            code = jobs.safe_code(exc)
            return self.outcome('pending_review' if code in {'review_gate_busy', 'review_refresh_timeout'} else 'blocked', code)

    def begin(self, payload):
        continuation = payload.get('continuation', False)
        if type(continuation) is not bool:
            raise ValueError('invalid_continuation')
        event = payload.get('event_id') or ''
        if continuation and not valid_hash(event):
            raise ValueError('invalid_event_id')
        if not continuation and event:
            raise ValueError('unexpected_event_id')
        resume = payload.get('resume', False)
        if type(resume) is not bool or (resume and continuation):
            raise ValueError('invalid_resume')
        mapping = None
        if resume:
            index = self.conversation_path(payload['origin'], payload.get('conversation_id'))
            if not index.exists():
                raise ValueError('no_pending_review')
            entry = read_json(index)
            if entry.get('status') != 'pending_review':
                raise ValueError('no_pending_review')
            mapping = {'task_id': entry['task_id'], 'kind': 'user_resume'}
            self.reply['task_id'] = entry['task_id']
        if continuation and (self.root / 'events' / (event + '.json')).exists():
            mapping = read_json(self.root / 'events' / (event + '.json'))
            self.reply['task_id'] = mapping['task_id']
        task_id = self.reply['task_id']
        dest = self.task_dir(task_id)
        state_path = dest / 'task.json'
        request = None
        if not mapping or mapping.get('kind') == 'collaboration':
            request = text_field(payload.get('request_text'), 'request_text_missing').encode('utf-8')
            if sha(request) != payload.get('request_sha256'):
                raise ValueError('request_hash_mismatch')
        if state_path.exists():
            task = read_json(state_path)
            if task['origin'] != payload['origin']:
                raise ValueError('origin_mismatch')
            if not mapping and task['request_sha256'] != payload.get('request_sha256'):
                raise ValueError('task_id_conflict')
            if mapping and mapping.get('kind') == 'collaboration' and task['request_sha256'] != sha(request):
                raise ValueError('callback_request_changed')
        else:
            if mapping:
                raise ValueError('callback_task_missing')
            conversation = payload.get('conversation_id') or ''
            if not isinstance(conversation, str):
                raise ValueError('invalid_conversation_id')
            dest.mkdir(mode=0o700)
            (dest / 'revisions').mkdir(mode=0o700)
            immutable(dest / 'request.txt', request)
            task = {'task_id': task_id, 'origin': payload['origin'],
                    'request_sha256': sha(request), 'conversation_id': conversation,
                    'binding': None, 'active_revision': None, 'revisions': [], 'created': bridge.now()}
            order_path = self.root / 'task-sequence.json'
            order = read_json(order_path).get('last', 0) if order_path.exists() else 0
            if type(order) is not int or order < 0:
                raise ValueError('task_sequence_invalid')
            task['creation_order'] = order + 1
            atomic_json(order_path, {'last': task['creation_order']})
            atomic_json(state_path, task)
        if sha(read_bytes(dest / 'request.txt')) != task['request_sha256']:
            raise ValueError('original_request_modified')
        if continuation and not mapping:
            atomic_json(self.root / 'events' / (event + '.json'),
                        {'task_id': task_id, 'kind': 'collaboration', 'revision': None})
        self.describe_task(dest, task)
        if mapping and mapping.get('revision') and mapping['revision'] != task['active_revision']:
            return self.outcome('blocked', 'stale_callback_revision',
                                instructions='旧版本回传已定位原任务；不得恢复旧候选或借用旧审查。')
        if task['binding'] and payload.get('conversation_id') not in ('', None, task['conversation_id']):
            raise ValueError('conversation_mismatch')
        if resume and (not task['binding'] or task['conversation_id'] != payload.get('conversation_id')):
            raise ValueError('conversation_index_mismatch')
        return self.outcome('pending_review')

    def instructions(self, dest, task, note=None):
        base = '\n'.join([
            'Dorothy 的每项任务必须经过 Hermes 实际交叉审查；候选正文在核验通过前不会发布。',
            '固定任务 ID：' + task['task_id'],
            '环境：CC_REVIEW_TASK_ID、CC_REVIEW_TASK_DIR、CC_REVIEW_MANIFEST_PATH。',
            '先依据原始任务独立判断，再写 ' + str(dest / 'manifest.json') + '。',
            'manifest 格式：' + json.dumps({
                'schema': MANIFEST_SCHEMA, 'task_id': task['task_id'],
                'mode': 'answer_only 或 artifacts（二选一）',
                'acceptance': ['具体目标与验收条件'], 'independent_judgment': '查看 Hermes 结论前的独立初判',
                'artifacts': [{'path': '交付文件绝对路径', 'sha256': '文件原字节 SHA-256'}],
                'test_evidence': [{'path': '测试证据绝对路径', 'sha256': 'SHA-256', 'description': '验证范围及结果'}]
            }, ensure_ascii=False),
            '纯答案须明确 mode=answer_only、artifacts=[]；无文件测试证据时 test_evidence=[]。不自动推定纯答案。',
            'acceptance 只写用户交付的验收条件；内部 manifest 写入顺序、未来复核回执和发布状态由宿主另行核验，不额外变成用户答案的验收前提。',
            '引用原文、规则或代码时，使用 references=[{"path":"工作区内源文件绝对路径"}] 提供依据；程序负责计算哈希并冻结副本。references 不是交付制品，也不是测试证据。',
            '宿主自动冻结工作区 AGENTS.md、agent/dorothy-collaboration.md 和有限运行证据；这些材料可支持成员称呼、协作规则及本次配置/观察值，不证明模型能力或未发生的审查。',
            '只在当前 manifest 路径写清单。首次候选或修改候选时，清单写入后给出候选正文；已有候选无需修改时，按下方 preserve_candidate 契约回应即可。',
            '不要把清单、工具操作或等待审核当作候选答案。缺清单、缺候选或缺回应时应在同一固定任务内补齐，不新建任务、不要求用户填写内部文件。',
            '候选正文保持稳定；不要将运行状态、IPC 通知或审查完成附注混入候选。门禁会附带独立机器回执。',
        ])
        if task.get('last_draft'):
            base += '\n已持久保存的最近候选草稿（未复核）：' + str(dest / task['last_draft']['path'])
        revision_id = task.get('active_revision')
        if not revision_id:
            return base
        revision_dir = dest / 'revisions' / revision_id
        revision = read_json(revision_dir / 'revision.json')
        if note is None:
            note = ('hermes_launch_failed' if (revision_dir / 'failed-review.json').exists() else
                    '已冻结实际答复；仍须完整核验并由你实际阅读回应' if (revision_dir / 'hermes-response.txt').exists()
                    else 'hermes_review_pending')
        response = {'schema': RESPONSE_SCHEMA, 'task_id': task['task_id'],
                    'revision': revision_id, 'candidate_sha256': revision['candidate_sha256'],
                    'manifest_sha256': revision['manifest_sha256'],
                    'hermes_response_sha256': revision.get('hermes_response_sha256', '读取实际正文后计算'),
                    'review_read': True, 'preserve_candidate': True,
                    'assessment': '实际阅读审查后的独立核对、异议与限制',
                    'responses': [], 'limitations': []}
        return base + '\n' + '\n'.join([
            '这是原任务接续，不能把 IPC 通知本身当成新的交付答案。',
            '当前候选：' + str(revision_dir / 'materials/candidate.txt'),
            '当前固定 manifest：' + str(revision_dir / 'manifest.json'),
            'Hermes 原始答复：' + str(revision_dir / 'hermes-response.txt'),
            '审查状态：' + (note or '已取得；仍需你实际阅读并回应'),
            '若审查状态为 hermes_launch_failed，上一作业已确认未启动模型且进程已释放；在 manifest 添加 retry_review={"job_id":"' + revision['job_id'] + '","reason":"已核实的恢复依据"} 后形成新修订重审。不得重投不明作业。',
            '实际读完审查后写 ' + str(dest / 'response.json') + '：' + json.dumps(response, ensure_ascii=False),
            'responses 对每条 finding 和 followup 各含 finding_id、resolution（verified/not_applicable/disputed/pending）、assessment、evidence（非空字符串数组）。无发现可用空数组。',
            '有修改就更新 manifest 中的源哈希和测试证据，提交新候选供重新审查。关键异议须补证据并复核，不能只写同意。',
            '无须修改且已实际回应时设置 preserve_candidate=true，宿主会直接读取、核验并发布固定候选原文；无需由你逐字重打一遍。',
            '需要修改候选或材料时设置 preserve_candidate=false，更新 manifest 并输出新候选；不要用 preserve_candidate=true 绕过修订。',
        ])

    def manifest(self, dest, payload):
        path = dest / 'manifest.json'
        requested = payload.get('manifest_path')
        if not isinstance(requested, str) or Path(requested).absolute() != path:
            raise ValueError('manifest_path_mismatch')
        if not path.exists():
            return None
        raw = read_bytes(path)
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get('schema') != MANIFEST_SCHEMA:
            raise ValueError('manifest_schema_mismatch')
        if value.get('task_id') != self.reply['task_id']:
            raise ValueError('manifest_task_mismatch')
        strings(value.get('acceptance'), True)
        text_field(value.get('independent_judgment'), 'independent_judgment_missing')
        artifacts, tests, references = value.get('artifacts'), value.get('test_evidence'), value.get('references', [])
        if (not all(isinstance(items, list) for items in (artifacts, tests, references)) or
                len(artifacts) + len(tests) + len(references) > 128):
            raise ValueError('invalid_manifest_files')
        mode = value.get('mode')
        if mode not in ('answer_only', 'artifacts') or (mode == 'answer_only' and artifacts) or (mode == 'artifacts' and not artifacts):
            raise ValueError('explicit_manifest_mode_required')
        files, total, seen = [], 0, set()
        for kind, entries in (('artifact', artifacts), ('test', tests), ('reference', references)):
            for index, item in enumerate(entries):
                if (not isinstance(item, dict) or
                        not (kind == 'reference' and 'sha256' not in item) and not valid_hash(item.get('sha256'))):
                    raise ValueError('invalid_manifest_file')
                source = Path(text_field(item.get('path'), 'invalid_manifest_path'))
                if (not source.is_absolute() or source.is_symlink() or
                        str(source.resolve()) != str(source) or
                        not source.is_relative_to(bridge.WORKSPACE.resolve()) or source in seen):
                    raise ValueError('manifest_source_outside_scope')
                if kind == 'test':
                    text_field(item.get('description'), 'test_description_missing')
                data = read_bytes(source)
                if item.get('sha256') is not None and sha(data) != item['sha256']:
                    raise ValueError('artifact_hash_mismatch')
                total += len(data)
                if total > MAX_MATERIALS:
                    raise ValueError('materials_too_large')
                seen.add(source)
                files.append({'source': str(source), 'snapshot': f'{kind}-{index:03d}-{source.name}',
                              'sha256': sha(data), 'data': data})
        return raw, value, files

    def bind_origin(self, task, payload, dest):
        conversation = text_field(payload.get('conversation_id'), 'conversation_id_missing')
        evidence = runtime_evidence(payload['runtime_evidence']) if 'runtime_evidence' in payload else {}
        binding = ipc.validate_binding(ipc.capture())
        if any(binding['target'][key] != task['origin'][key] for key in task['origin']):
            raise ValueError('captured_origin_mismatch')
        if binding['target']['agent_session_id'] != conversation:
            raise ValueError('captured_conversation_mismatch')
        if task['binding'] and task['binding'] != binding:
            raise ValueError('captured_binding_changed')
        if task['conversation_id'] and task['conversation_id'] != conversation:
            raise ValueError('conversation_mismatch')
        previous = task.get('runtime_evidence', {})
        if any(key in previous and previous[key] != value for key, value in evidence.items()):
            raise ValueError('runtime_evidence_changed')
        if not task['binding'] or any(key not in previous for key in evidence):
            task.update(binding=binding, conversation_id=conversation, runtime_evidence=previous | evidence)
            atomic_json(dest / 'task.json', task)
        self.index_conversation(task)

    def bind(self, payload):
        dest, task = self.task_context(payload)
        self.bind_origin(task, payload, dest)
        self.describe_task(dest, task)
        return self.outcome('pending_review')

    def refresh(self, payload):
        dest, task = self.task_context(payload)
        self.bind_origin(task, payload, dest)
        revision_id = task.get('active_revision')
        if not revision_id:
            reason = 'manifest_missing' if not (dest / 'manifest.json').exists() else 'candidate_missing'
            return self.outcome('pending_review', reason)
        if payload.get('revision', revision_id) != revision_id:
            raise ValueError('refresh_revision_mismatch')
        directory = dest / 'revisions' / revision_id
        revision = read_json(directory / 'revision.json')
        try:
            report, reason = self.collect_review(task, directory, revision)
            if report:
                self.verify_files(directory, revision)
        except Exception as exc:
            reason = jobs.safe_code(exc)
        else:
            if report:
                reason = 'hermes_review_ready' if self.review_ready(report, revision) else 'hermes_changes_or_unresolved'
                required = {f['finding_id'] for f in revision['prior_findings']}
                if reason == 'hermes_review_ready' and not required.issubset({f['finding_id'] for f in report['followup']}):
                    reason = 'prior_findings_not_verified'
        self.describe_task(dest, task)
        self.reply['instructions'] = self.instructions(dest, task, note=reason)
        return self.outcome('pending_review', reason)

    def freeze(self, task, dest, candidate, raw_manifest, manifest, files):
        seed = {'task_id': task['task_id'], 'sequence': len(task['revisions']) + 1,
                'candidate_sha256': sha(candidate), 'manifest_sha256': sha(raw_manifest)}
        revision_id = sha(json_bytes(seed))
        revision_dir = dest / 'revisions' / revision_id
        revision_dir.mkdir(mode=0o700, exist_ok=True)
        materials = revision_dir / 'materials'
        materials.mkdir(mode=0o700, exist_ok=True)
        immutable(revision_dir / 'manifest.json', raw_manifest)
        immutable(materials / 'candidate.txt', candidate)
        immutable(materials / 'request.txt', read_bytes(dest / 'request.txt'))
        protocol_path = Path(__file__).resolve().parent.parent / 'references/review-protocol.md'
        immutable(materials / 'review-protocol.md', read_bytes(protocol_path))
        descriptors = []
        for item in files:
            immutable(materials / item['snapshot'], item['data'])
            descriptors.append({key: value for key, value in item.items() if key != 'data'})
        shared_rules = []
        for relative, snapshot in (('AGENTS.md', 'workspace-AGENTS.md'),
                                   ('agent/dorothy-collaboration.md', 'dorothy-collaboration.md')):
            source = bridge.WORKSPACE.resolve() / relative
            if source.is_symlink() or source.resolve() != source:
                raise ValueError('shared_rule_source_outside_scope')
            data = read_bytes(source)
            immutable(materials / snapshot, data)
            descriptor = {'source': str(source), 'snapshot': snapshot, 'sha256': sha(data)}
            descriptors.append(descriptor)
            shared_rules.append(descriptor)
        # Author judgment is frozen privately; initial reviewer inputs exclude it.
        public = {key: value for key, value in manifest.items() if key != 'independent_judgment'}
        public['files'] = descriptors
        immutable(materials / 'review-input.json', json_bytes(public))
        immutable(materials / 'host-evidence.json', json_bytes({
            'candidate_sha256': sha(candidate), 'manifest_sha256': sha(raw_manifest),
            'manifest_validated': True, 'file_hashes_verified': True,
            'runtime_evidence': task.get('runtime_evidence', {}), 'shared_rules': shared_rules,
            'runtime_evidence_scope': '仅保存宿主本次请求提供的白名单字段：configured_model、effort、mode 是配置值；observed_model 是 CLI 事件披露值。未提供字段表示未采集，不能推断有效值；不读取完整配置或凭据，不证明性能或回答正确性。',
            'scope': '宿主已验证原请求、manifest 格式与绑定、候选及文件原字节；未证明工具执行顺序、模型语义或未来审查结果。',
        }))
        prior_findings = []
        previous = task.get('active_revision')
        if previous:
            prev_dir = dest / 'revisions' / previous
            prev = read_json(prev_dir / 'revision.json')
            prior_findings = list(prev.get('prior_findings', []))
            previous_report = prev_dir / 'hermes-response.txt'
            failed_review = prev_dir / 'failed-review.json'
            if failed_review.exists():
                immutable(materials / 'previous-review-failure.json', read_bytes(failed_review))
            if previous_report.exists():
                raw = read_bytes(previous_report)
                if sha(raw) != prev.get('hermes_response_sha256'):
                    raise ValueError('previous_review_modified')
                try:
                    report = self.parse_review(raw, task, prev)
                    if not prev.get('native_evidence_sha256'):
                        raise ValueError('previous_native_review_unverified')
                except (ValueError, KeyError, TypeError):
                    prior_findings.append({'finding_id': previous + '/report-validation', 'kind': 'risk',
                                           'judgment': 'unresolved', 'disposition': 'unverified', 'critical': True,
                                           'claim': '上一轮报告或原生参与证据未通过核验，需要本轮独立重审并回应其可见内容。',
                                           'evidence': ['previous-review.json']})
                else:
                    prior = {f['finding_id']: f for f in prior_findings}
                    closed = {f['finding_id'] for f in report['followup']
                              if self.closed(f, prior.get(f['finding_id']))}
                    prior_findings = [f for f in prior_findings if f['finding_id'] not in closed]
                    for finding in report['findings']:
                        if not self.closed(finding):
                            prior_findings.append(finding | {'finding_id': previous + '/' + finding['id']})
                immutable(materials / 'previous-review.json', raw)
            if (dest / 'response.json').exists():
                immutable(materials / 'previous-author-response.json', read_bytes(dest / 'response.json'))
        immutable(materials / 'prior-findings.json', json_bytes({'findings': prior_findings}))
        revision = seed | {'revision': revision_id, 'files': descriptors, 'previous_revision': previous,
                           'prior_findings': prior_findings, 'submit_state': 'not_started',
                           'job_id': 'dorothy-' + revision_id[:48], 'created': bridge.now()}
        revision['materials'] = [{'path': path.name, 'sha256': sha(read_bytes(path))}
                                 for path in sorted(materials.iterdir())]
        task_text = self.review_task(task, revision, materials)
        immutable(revision_dir / 'review-task.md', task_text.encode('utf-8'))
        revision['task_sha256'] = sha(task_text.encode('utf-8'))
        atomic_json(revision_dir / 'revision.json', revision)
        task['active_revision'] = revision_id
        task['revisions'].append(revision_id)
        atomic_json(dest / 'task.json', task)
        return revision_dir, revision

    def review_task(self, task, revision, materials):
        expected = {key: revision[key] for key in ('task_id', 'revision', 'candidate_sha256', 'manifest_sha256')}
        sample = expected | {'schema': REVIEW_SCHEMA, 'verdict': 'pass',
                             'scope': '实际已读材料及未覆盖范围', 'independent_initial': '主 Agent 的独立初判',
                             'reviewers': [{'agent_id': '该真实 subagent 调用的 description 原文（可核对 callId）', 'perspective': '审查视角',
                                            'status': 'completed', 'independent_initial': '交换意见前初判',
                                            'checked_materials': ['candidate.txt'], 'findings': []}],
                             'findings': [], 'followup': [], 'limitations': []}
        return '\n'.join([
            '# Dorothy 固定版本交叉审查（Hermes）',
            '只读文件审查；不执行 Shell、测试、写文件、联网搜索、消息发送或修改任何配置。列目录也不得执行 Shell（包括 ls、pwd、find）。',
            '只允许读取本任务提供的材料目录：' + str(materials),
            '固定材料文件列表：' + json.dumps([item['path'] for item in revision['materials']], ensure_ascii=False),
            '使用上述已知文件列表和文件读取工具，不调用 Shell 或命令执行工具探查目录。',
            'cwd 与独立 home 不提供读取隔离；不得读取上级目录、凭据、用户历史、其他任务或原始源文件。',
            '当前任务：' + json.dumps(expected, ensure_ascii=False),
            '先阅读 request.txt、candidate.txt、review-input.json、host-evidence.json、workspace-AGENTS.md、dorothy-collaboration.md、固定 artifact/test/reference 副本、review-protocol.md。',
            '原路径仅标注出处；审查固定副本。测试证据只做文件核对，不能声称执行了测试。',
            'host-evidence.json 是宿主的有限机械验证记录；无需为已由宿主校验的哈希重复运行 Shell，也不能据此声称你亲自执行了工具。',
            '共同规则原文用于核对成员称呼、模型选择和协作要求；运行字段只支持本次配置/观察值，不外推真实性能、供应商内部路由或未来任务。缺字段按未验证列明，不要求读取配置或凭据。',
            '审查当前候选是否满足用户实际要求。未来 Hermes 回执、未来发布状态不是候选的前置证据，不以缺少本轮尚未完成的审查回执构造循环门槛。',
            '只在违反具体要求或关键事实缺证时提出阻塞发现；普通范围限制放 limitations，偏好不得升级为必改缺陷。',
            '若单列不影响验收的剩余风险或可选偏好，须明确 kind=risk/preference、critical=false、disposition=no_action，并以 claim/evidence 说明为何无需处置；可保留 confirmed/unresolved 判定，不得谎称已经证伪或修复。Dorothy 仍须逐项实际回应。',
            '未决缺陷、关键风险、needs_change/unverified 或未完成的修复必须阻断；历史关键发现、事实缺陷和已要求处置的条目，须经 verified_closed 或 not_confirmed+no_action 证据关闭，不能仅降低 critical、改称普通风险/偏好或改成 no_action。',
            '至少调用两个实际子 Agent，以不同视角独立先判；使用同步返回等待结果，不可派发后提前 final。',
            '每次 subagent 显式 run_in_background=false，description 必须互异；子任务 prompt 必须含上方材料目录完整路径和 revision。',
            '向每个子 Agent 传递本任务全部材料边界、固定版本、只读限制和 review-protocol.md 适用条款。',
            '主 Agent 也先独立判断，再综合子 Agent 结果；不能只投票或只审作者摘要。',
            '初判完成后再阅读存在的 previous-review.json、previous-author-response.json、prior-findings.json，',
            '以源文、代码与已提供验证证据核对关键修改；在 followup 中逐条回答 prior-findings 的 finding_id。',
            '按证据裁决分歧，不要求强行一致。报告实际子 Agent 标识、视角、初判、发现及未覆盖事项。',
            '缺材料、缺子 Agent 实际返回、关键未决或不能验证修复时用 unresolved/changes_required，不要输出 pass。',
            '最终回答只能是一个 JSON 对象：首个与末个非空白字符必须分别是 { 和 }，不加 Markdown 围栏、前导语或结语。',
            '不要在最终 JSON 前后写“已经完成审查”“两个审查者已返回”“下面输出 JSON”等进度或说明文字。',
            '格式如下（reviewers 实际至少两条且标识及视角不同）：',
            json.dumps(sample, ensure_ascii=False, indent=2),
            'reviewers[].findings 必须是字符串摘要数组，例如 ["F1：具体发现摘要"] 或 []；不得在该数组中放 JSON 对象。',
            '详细发现对象只放在顶层 findings 或 followup，子审发现摘要保留其结论和对应发现标识。',
            'verdict: pass/changes_required/unresolved。findings 每条必含 id、kind(defect/risk/preference)、',
            'judgment(confirmed/not_confirmed/unresolved)、disposition(verified_closed/no_action/needs_change/unverified)、',
            'critical(bool)、claim、evidence(非空字符串数组)。修复后仍保留原问题 confirmed 的判定。',
            'followup 条目用 finding_id 代替 id，其余字段相同；允许明确证据证明不成立，不把一致意见当证据。',
            '无发现 findings=[]；子 Agent 无发现同样允许，不设发现配额。limitations 必须是数组。',
            '本协议验证参与声明与证据绑定，不将声明或模型一致当成正确性的数学保证。',
        ]) + '\n'

    def verify_files(self, revision_dir, revision):
        for item in revision['files']:
            if sha(read_bytes(Path(item['source']))) != item['sha256']:
                raise ValueError('source_changed_after_review')
        for item in revision['materials']:
            if sha(read_bytes(revision_dir / 'materials' / item['path'])) != item['sha256']:
                raise ValueError('frozen_material_modified')
        if sha(read_bytes(revision_dir / 'manifest.json')) != revision['manifest_sha256']:
            raise ValueError('frozen_manifest_modified')
        if sha(read_bytes(revision_dir / 'review-task.md')) != revision['task_sha256']:
            raise ValueError('review_task_modified')

    def reconcile_submission(self, task, revision_dir, revision):
        """Resolve a late durable receipt without launching or changing any job."""
        if revision['submit_state'] == 'submitted':
            return None
        job_dir = jobs.job_path(self.runtime, revision['job_id'])
        if revision['submit_state'] not in {'submitting', 'unknown'} or not (job_dir / 'receipt.json').is_file():
            return 'submission_unconfirmed'
        try:
            request = read_json(job_dir / 'request.json')
            if (request.get('task_sha256') != revision['task_sha256'] or
                    request.get('profile') != PROFILE or
                    request.get('reasoning_effort') != EFFORT or
                    request.get('cwd') != str((revision_dir / 'materials').resolve()) or
                    request.get('delivery') != 'conversation' or
                    request.get('callback_target') != task['binding']):
                raise ValueError('submission_receipt_mismatch')
            receipt = read_json(job_dir / 'receipt.json')
            if receipt.get('job_id') != revision['job_id']:
                raise ValueError('submission_receipt_mismatch')
        except Exception:
            return 'submission_unconfirmed'
        revision['submit_state'] = 'submitted'
        atomic_json(revision_dir / 'revision.json', revision)
        return None

    def submit(self, task, revision_dir, revision):
        if revision['submit_state'] != 'not_started':
            return self.reconcile_submission(task, revision_dir, revision)
        job_dir = jobs.job_path(self.runtime, revision['job_id'])
        reason = capacity_reason(self.runtime, (revision_dir / 'materials').resolve())
        if reason:
            return reason
        seed = {'job_dir': str(job_dir.resolve()), 'task_sha256': revision['task_sha256'],
                'target': task['binding']['target']}
        # Exact ipc.build_input event algorithm; registration precedes any worker launch.
        event = bridge.digest(json.dumps(seed, sort_keys=True).encode('utf-8'))
        mapping = {'task_id': task['task_id'], 'revision': revision['revision'],
                   'kind': 'hermes', 'job_id': revision['job_id']}
        event_path = self.root / 'events' / (event + '.json')
        if event_path.exists() and read_json(event_path) != mapping:
            raise ValueError('callback_event_conflict')
        atomic_json(event_path, mapping)
        revision.update(submit_state='submitting', event_id=event)
        atomic_json(revision_dir / 'revision.json', revision)
        try:
            receipt = jobs.submit(revision_dir / 'review-task.md', revision['job_id'],
                                  timeout=TIMEOUT, runtime=self.runtime, title='Dorothy 联合复核',
                                  cwd=str(revision_dir / 'materials'), profile=PROFILE,
                                  reasoning_effort=EFFORT, delivery='conversation',
                                  callback_target=task['binding'])
        except Exception:
            # A published request may already be executing. Never retry an uncertain submission.
            revision['submit_state'] = 'submitted' if (job_dir / 'receipt.json').exists() else 'unknown'
            atomic_json(revision_dir / 'revision.json', revision)
            return 'submission_unconfirmed'
        if receipt.get('job_id') != revision['job_id']:
            revision['submit_state'] = 'unknown'
            atomic_json(revision_dir / 'revision.json', revision)
            return 'submission_receipt_mismatch'
        revision['submit_state'] = 'submitted'
        atomic_json(revision_dir / 'revision.json', revision)
        return 'hermes_review_pending'

    def collect_review(self, task, revision_dir, revision):
        reason = self.reconcile_submission(task, revision_dir, revision)
        if reason:
            return None, reason
        record = jobs.status(revision['job_id'], self.runtime)
        if record['status'] in jobs.ACTIVE:
            return None, 'hermes_review_pending'
        if record['status'] != 'completed':
            if self.record_failed_launch(task, revision_dir, revision, record):
                return None, 'hermes_launch_failed'
            raise ValueError('hermes_job_not_completed')
        result = jobs.result(revision['job_id'], self.runtime)
        run_dir, run = bridge.load_run(result['run_dir'], self.runtime)
        request = read_json(jobs.job_path(self.runtime, revision['job_id']) / 'request.json')
        for document in (record, result, request, run):
            if document.get('profile') != PROFILE or document.get('reasoning_effort') != EFFORT:
                raise ValueError('review_model_settings_mismatch')
            if document.get('cwd') != str(revision_dir / 'materials'):
                raise ValueError('review_cwd_mismatch')
        if (request.get('delivery') != 'conversation' or request.get('callback_target') != task['binding'] or
                request.get('task_sha256') != revision['task_sha256'] or
                run.get('task_sha256') != revision['task_sha256'] or
                sha(read_bytes(run_dir / 'task.md')) != revision['task_sha256']):
            raise ValueError('review_request_mismatch')
        if run.get('status') != 'completed' or type(run.get('returncode')) is not int or run['returncode'] != 0:
            raise ValueError('review_exit_not_successful')
        cleanup = result.get('cleanup', {})
        lock_name = 'instance_bridge_lock' if record.get('instance_id') else 'shared_bridge_lock'
        if (cleanup.get('state') != 'released' or cleanup.get('dsh_returncode') != 0 or
                any(cleanup.get(key) != 'absent' for key in ('worker_process', 'dsh_process', 'dsh_process_group')) or
                any(cleanup.get(key) != 'released' for key in ('worker_lock', lock_name))):
            return None, 'hermes_cleanup_pending'
        raw = read_bytes(run_dir / 'response.md')
        if (sha(raw) != result.get('response_sha256') or sha(raw) != run.get('response_sha256') or
                result.get('response') != raw.decode('utf-8')):
            raise ValueError('review_response_hash_mismatch')
        # Preserve the real visible reply even if its schema/participation fails,
        # so the author can inspect and repair the task without inventing a reply.
        immutable(revision_dir / 'hermes-response.txt', raw)
        if revision.get('hermes_response_sha256') not in (None, sha(raw)):
            raise ValueError('review_response_changed')
        revision.update(hermes_response_sha256=sha(raw), hermes_run_dir=str(run_dir))
        atomic_json(revision_dir / 'revision.json', revision)
        native = verify_native(run, read_bytes(run_dir / 'task.md').decode('utf-8'), raw.decode('utf-8'),
                               self.runtime, revision_dir / 'materials', revision['revision'])
        report = self.parse_review(raw, task, revision)
        aliases = {alias: item['call_id'] for item in native['subreviews']
                   for alias in (item['call_id'], item['child_session_id'], item['description'])}
        linked = [aliases.get(reviewer['agent_id']) for reviewer in report['reviewers']]
        if None in linked or len(set(linked)) != len(linked):
            raise ValueError('report_subreviews_not_in_native_source')
        immutable(revision_dir / 'native-evidence.json', json_bytes(native))
        revision['native_evidence_sha256'] = sha(json_bytes(native))
        atomic_json(revision_dir / 'revision.json', revision)
        return report, ''

    def record_failed_launch(self, task, revision_dir, revision, record):
        """Only a verified pre-model launch failure can permit a replacement review."""
        if record.get('status') not in ('process_failed', 'launch_failed') or not record.get('run_dir'):
            return False
        run_dir, run = bridge.load_run(record['run_dir'], self.runtime)
        request = read_json(jobs.job_path(self.runtime, revision['job_id']) / 'request.json')
        if (run.get('session_id') is not None or run.get('status') != record['status'] or
                (run.get('returncode') != 127 and not (run['status'] == 'launch_failed' and run.get('returncode') is None)) or
                (run_dir / 'response.md').exists()):
            return False
        if any(document.get('profile') != PROFILE or document.get('reasoning_effort') != EFFORT or
               document.get('cwd') != str(revision_dir / 'materials') for document in (record, request, run)):
            raise ValueError('review_request_mismatch')
        if (request.get('task_sha256') != revision['task_sha256'] or
                request.get('delivery') != 'conversation' or request.get('callback_target') != task['binding'] or
                run.get('task_sha256') != revision['task_sha256'] or
                sha(read_bytes(run_dir / 'task.md')) != revision['task_sha256']):
            raise ValueError('review_request_mismatch')
        cleanup = record.get('cleanup', {})
        lock_name = 'instance_bridge_lock' if record.get('instance_id') else 'shared_bridge_lock'
        if (cleanup.get('state') != 'released' or
                any(cleanup.get(key) != 'absent' for key in ('worker_process', 'dsh_process', 'dsh_process_group')) or
                any(cleanup.get(key) != 'released' for key in ('worker_lock', lock_name))):
            return False
        immutable(revision_dir / 'failed-review.json', json_bytes({
            'job_id': revision['job_id'], 'status': record['status'], 'returncode': run.get('returncode'),
            'task_sha256': revision['task_sha256'], 'model_started': False, 'processes_released': True,
            'scope': '作业在报告模型会话前启动失败；无审查答复。原作业与回执保留，不重投不明请求。',
        }))
        return True

    def parse_review(self, raw, task, revision):
        report = json.loads(raw)
        if not isinstance(report, dict) or report.get('schema') != REVIEW_SCHEMA:
            raise ValueError('hermes_report_schema_mismatch')
        for key in ('task_id', 'revision', 'candidate_sha256', 'manifest_sha256'):
            if report.get(key) != revision[key]:
                raise ValueError('hermes_report_binding_mismatch')
        if report.get('verdict') not in ('pass', 'changes_required', 'unresolved'):
            raise ValueError('hermes_verdict_invalid')
        text_field(report.get('scope'))
        text_field(report.get('independent_initial'))
        reviewers = report.get('reviewers')
        if not isinstance(reviewers, list) or len(reviewers) < 2:
            raise ValueError('hermes_actual_subreviews_missing')
        identifiers, perspectives = set(), set()
        for reviewer in reviewers:
            if not isinstance(reviewer, dict) or reviewer.get('status') != 'completed':
                raise ValueError('hermes_subreview_incomplete')
            identifiers.add(text_field(reviewer.get('agent_id')))
            perspectives.add(text_field(reviewer.get('perspective')).strip())
            text_field(reviewer.get('independent_initial'))
            strings(reviewer.get('checked_materials'), True)
            strings(reviewer.get('findings'))
        if len(identifiers) != len(reviewers) or len(perspectives) < 2:
            raise ValueError('hermes_distinct_subreviews_missing')
        for name, key in (('findings', 'id'), ('followup', 'finding_id')):
            findings = report.get(name)
            if not isinstance(findings, list):
                raise ValueError('hermes_findings_missing')
            ids = set()
            for finding in findings:
                if not isinstance(finding, dict):
                    raise ValueError('hermes_finding_invalid')
                identifier = text_field(finding.get(key))
                if identifier in ids:
                    raise ValueError('duplicate_finding_id')
                ids.add(identifier)
                if (finding.get('kind') not in ('defect', 'risk', 'preference') or
                        finding.get('judgment') not in ('confirmed', 'not_confirmed', 'unresolved') or
                        finding.get('disposition') not in ('verified_closed', 'no_action', 'needs_change', 'unverified') or
                        type(finding.get('critical')) is not bool):
                    raise ValueError('hermes_finding_invalid')
                text_field(finding.get('claim'))
                strings(finding.get('evidence'), True)
        strings(report.get('limitations'))
        return report

    @staticmethod
    def closed(finding, previous=None):
        # Review judgment and release disposition are separate: an explicitly
        # accepted, noncritical risk need not be declared disproved or repaired.
        # The author must still respond, and prior blockers cannot be relabeled.
        prior_blocker = previous and (previous['critical'] or previous['kind'] == 'defect' or
                                      previous['disposition'] in ('needs_change', 'unverified'))
        if (finding['kind'] in ('risk', 'preference') and not finding['critical'] and
                finding['disposition'] == 'no_action' and not prior_blocker):
            return True
        if finding['judgment'] == 'unresolved':
            return False
        if finding['disposition'] == 'verified_closed':
            return True
        return finding['disposition'] == 'no_action' and finding['judgment'] == 'not_confirmed'

    def review_ready(self, report, revision):
        prior = {f['finding_id']: f for f in revision['prior_findings']}
        return (report['verdict'] == 'pass' and
                all(self.closed(f) for f in report['findings']) and
                all(self.closed(f, prior.get(f['finding_id'])) for f in report['followup']))

    def author_response(self, dest, revision, report):
        path = dest / 'response.json'
        if not path.exists():
            return None, 'dorothy_response_missing'
        raw = read_bytes(path)
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, 'dorothy_response_invalid_json'
        if not isinstance(value, dict) or value.get('schema') != RESPONSE_SCHEMA:
            raise ValueError('dorothy_response_schema_mismatch')
        for key in ('task_id', 'revision', 'candidate_sha256', 'manifest_sha256', 'hermes_response_sha256'):
            if value.get(key) != revision[key]:
                return None, 'dorothy_response_binding_mismatch'
        if value.get('review_read') is not True:
            return None, 'dorothy_review_not_read'
        if value.get('preserve_candidate') is False:
            return None, 'candidate_revision_required'
        if 'preserve_candidate' in value and type(value['preserve_candidate']) is not bool:
            return None, 'candidate_decision_missing'
        text_field(value.get('assessment'), 'dorothy_assessment_missing')
        strings(value.get('limitations'))
        responses = value.get('responses')
        if not isinstance(responses, list):
            raise ValueError('dorothy_responses_missing')
        expected = {f['id']: f for f in report['findings']}
        expected.update({f['finding_id']: f for f in report['followup']})
        seen = set()
        for response in responses:
            if not isinstance(response, dict):
                raise ValueError('dorothy_response_invalid')
            key = response.get('finding_id')
            if key not in expected or key in seen:
                raise ValueError('dorothy_response_finding_mismatch')
            seen.add(key)
            text_field(response.get('assessment'))
            strings(response.get('evidence'), True)
            resolution = response.get('resolution')
            if resolution not in ('verified', 'not_applicable', 'disputed', 'pending'):
                raise ValueError('dorothy_resolution_invalid')
            if resolution in ('disputed', 'pending'):
                return None, 'dorothy_findings_unresolved'
            if resolution == 'not_applicable' and not (expected[key]['kind'] == 'preference' or expected[key]['judgment'] == 'not_confirmed'):
                return None, 'dorothy_resolution_requires_review'
        if seen != set(expected):
            return None, 'dorothy_findings_not_answered'
        return raw, ''

    def check(self, payload):
        dest, task = self.task_context(payload)
        self.bind_origin(task, payload, dest)
        if not payload.get('candidate_text'):
            return self.outcome('pending_review', 'candidate_missing')
        candidate = text_field(payload.get('candidate_text'), 'candidate_missing').encode('utf-8')
        self.reply['candidate_sha256'] = sha(candidate)
        if sha(candidate) != payload.get('candidate_sha256'):
            raise ValueError('candidate_hash_mismatch')
        drafts = dest / 'drafts'
        if drafts.is_symlink():
            raise ValueError('draft_path_invalid')
        drafts.mkdir(exist_ok=True, mode=0o700)
        draft_path = drafts / (sha(candidate) + '.txt')
        immutable(draft_path, candidate)
        task['last_draft'] = {'path': str(draft_path.relative_to(dest)), 'sha256': sha(candidate), 'status': 'unreviewed'}
        atomic_json(dest / 'task.json', task)
        self.index_conversation(task, 'pending_review')
        selected = self.manifest(dest, payload)
        if selected is None:
            return self.outcome('pending_review', 'manifest_missing', instructions=self.instructions(dest, task))
        raw_manifest, manifest, files = selected
        self.reply['manifest_sha256'] = sha(raw_manifest)
        revision_dir, revision = None, None
        if task['active_revision']:
            revision_dir = dest / 'revisions' / task['active_revision']
            revision = read_json(revision_dir / 'revision.json')
        if not revision or (revision['candidate_sha256'], revision['manifest_sha256']) != (sha(candidate), sha(raw_manifest)):
            if (revision and revision['submit_state'] != 'not_started' and
                    not (revision_dir / 'hermes-response.txt').exists()):
                # A new candidate cannot discard a submitted review whose findings
                # have not been collected yet. Preserve invalid real replies too.
                try:
                    _, reason = self.collect_review(task, revision_dir, revision)
                except Exception:
                    if not (revision_dir / 'hermes-response.txt').exists():
                        raise
                else:
                    if reason == 'hermes_launch_failed':
                        retry = manifest.get('retry_review')
                        if (not isinstance(retry, dict) or retry.get('job_id') != revision['job_id'] or
                                not isinstance(retry.get('reason'), str) or not retry['reason'].strip()):
                            return self.outcome('pending_review', reason,
                                                instructions=self.instructions(dest, task))
                        consecutive = 0
                        for previous in reversed(task['revisions']):
                            if not (dest / 'revisions' / previous / 'failed-review.json').exists():
                                break
                            consecutive += 1
                        if consecutive >= 3:
                            return self.outcome('blocked', 'review_launch_attempts_exhausted')
                    elif reason:
                        return self.outcome('pending_review', 'previous_' + reason,
                                            instructions='新草稿已保存；先核对上一轮实际回执，再冻结新版本并接续未关闭发现。')
            revision_dir, revision = self.freeze(task, dest, candidate, raw_manifest, manifest, files)
        self.reply.update(revision=revision['revision'], hermes_job_id=revision['job_id'])
        self.verify_files(revision_dir, revision)
        reason = self.submit(task, revision_dir, revision)
        if reason:
            return self.outcome('pending_review', reason,
                                instructions='等待 Hermes 回传；任务及固定候选已保留，不能报告完成。')
        report, reason = self.collect_review(task, revision_dir, revision)
        if not report:
            return self.outcome('pending_review', reason, instructions=self.instructions(dest, task, note=reason))
        self.reply['evidence_refs'] = [
            {'path': str(revision_dir / 'materials/candidate.txt'), 'sha256': revision['candidate_sha256']},
            {'path': str(revision_dir / 'manifest.json'), 'sha256': revision['manifest_sha256']},
            {'path': str(revision_dir / 'hermes-response.txt'), 'sha256': revision['hermes_response_sha256']},
            {'path': str(revision_dir / 'native-evidence.json'), 'sha256': sha(read_bytes(revision_dir / 'native-evidence.json'))},
        ]
        self.reply['instructions'] = self.instructions(dest, task)
        if not self.review_ready(report, revision):
            return self.outcome('blocked', 'hermes_changes_or_unresolved')
        required = {f['finding_id'] for f in revision['prior_findings']}
        if not required.issubset({f['finding_id'] for f in report['followup']}):
            return self.outcome('blocked', 'prior_findings_not_verified')
        response, reason = self.author_response(dest, revision, report)
        if response is None:
            return self.outcome('pending_review', reason)
        response_path = revision_dir / ('author-response-' + sha(response) + '.json')
        immutable(response_path, response)
        self.reply['evidence_refs'].append({'path': str(response_path), 'sha256': sha(response)})
        # Re-read sources and current manifest at the last possible gate boundary.
        self.verify_files(revision_dir, revision)
        if read_bytes(dest / 'manifest.json') != raw_manifest:
            raise ValueError('manifest_changed_during_check')
        result = self.outcome('verified', instructions='联合复核证据已核对；仅发布候选原文。')
        atomic_json(revision_dir / 'completion.json', result | {'checked_at': bridge.now()})
        self.index_conversation(task, 'verified')
        return result

    def finalize(self, payload):
        """Publish a reviewed file by reference after the author explicitly keeps it."""
        dest, task = self.task_context(payload)
        self.bind_origin(task, payload, dest)
        revision_id = task.get('active_revision')
        if not revision_id or payload.get('revision') != revision_id:
            raise ValueError('finalize_revision_mismatch')
        directory = dest / 'revisions' / revision_id
        revision = read_json(directory / 'revision.json')
        self.reply.update(revision=revision_id, candidate_sha256=revision['candidate_sha256'],
                          manifest_sha256=revision['manifest_sha256'], hermes_job_id=revision['job_id'])
        response_path = dest / 'response.json'
        if not response_path.exists():
            return self.outcome('pending_review', 'dorothy_response_missing')
        try:
            response = json.loads(read_bytes(response_path))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self.outcome('pending_review', 'dorothy_response_invalid_json')
        if not isinstance(response, dict) or response.get('schema') != RESPONSE_SCHEMA:
            return self.outcome('pending_review', 'dorothy_response_schema_mismatch')
        if response.get('preserve_candidate') is False:
            return self.outcome('pending_review', 'candidate_revision_required')
        if response.get('preserve_candidate') is not True:
            return self.outcome('pending_review', 'candidate_decision_missing')
        if task.get('last_draft', {}).get('sha256', revision['candidate_sha256']) != revision['candidate_sha256']:
            return self.outcome('pending_review', 'candidate_revision_required')
        if not (dest / 'manifest.json').exists():
            return self.outcome('pending_review', 'manifest_missing')
        if read_bytes(dest / 'manifest.json') != read_bytes(directory / 'manifest.json'):
            return self.outcome('pending_review', 'candidate_materials_changed')
        candidate_path = directory / 'materials/candidate.txt'
        candidate = read_bytes(candidate_path).decode('utf-8')
        result = self.check(payload | {'op': 'check', 'candidate_text': candidate,
                                      'candidate_sha256': revision['candidate_sha256'],
                                      'manifest_path': str(dest / 'manifest.json')})
        if result['allow_publish']:
            result['candidate_path'] = str(candidate_path)
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--review-root', type=Path, default=REVIEW_ROOT)
    parser.add_argument('--runtime', type=Path, default=bridge.RUNTIME)
    args = parser.parse_args()
    try:
        raw = sys.stdin.buffer.read(MAX_INPUT + 1)
        if len(raw) > MAX_INPUT:
            raise ValueError('input_too_large')
        payload = json.loads(raw)
        result = Gate(args.review_root, args.runtime).handle(payload)
    except Exception as exc:
        result = {'protocol': PROTOCOL, 'task_id': '', 'status': 'blocked', 'allow_publish': False,
                  'candidate_sha256': '', 'manifest_sha256': '', 'hermes_job_id': None,
                  'evidence_refs': [], 'instructions': '', 'reasons': [jobs.safe_code(exc)]}
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    sys.exit(main())
