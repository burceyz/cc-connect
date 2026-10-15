"""Bind read-only material paths separately from the execution directory."""
import hashlib
import json
from pathlib import Path
import re

POLICY = Path(__file__).with_name('read-sources.patch.yml')
FIELDS = ('read_sources', 'read_policy_sha256')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def resolve(values, workspace, *, require_exists=True):
    if values is None:
        return []
    if not isinstance(values, (list, tuple)) or not values or len(values) > 64:
        raise ValueError('invalid_read_sources')
    root = Path(workspace).resolve()
    result = []
    for value in values:
        if not isinstance(value, (str, Path)) or not str(value) or any(ord(c) < 32 for c in str(value)):
            raise ValueError('invalid_read_sources')
        path = Path(value).expanduser().resolve()
        if not path.is_relative_to(root):
            raise ValueError('source_outside_workspace')
        if require_exists and not (path.is_file() or path.is_dir()):
            raise ValueError('source_unavailable')
        result.append(str(path))
    return sorted(set(result))


def binding(paths):
    if not paths:
        return {}, None
    if POLICY.is_symlink() or not POLICY.is_file():
        raise ValueError('read_policy_unavailable')
    data = POLICY.read_bytes()
    return {'read_sources': paths, 'read_policy_sha256': digest(data)}, data


def record(value):
    if not any(name in value for name in FIELDS):
        return {}
    paths, policy = value.get('read_sources'), value.get('read_policy_sha256')
    if (not isinstance(paths, list) or not paths or len(paths) > 64 or
            any(not isinstance(p, str) or not Path(p).is_absolute() for p in paths) or
            paths != sorted(set(paths)) or
            not isinstance(policy, str) or not re.fullmatch('[a-f0-9]{64}', policy)):
        raise ValueError('read_sources_invalid')
    return {name: value[name] for name in FIELDS}


def validate(actual, expected):
    if record(actual) != record(expected):
        raise ValueError('read_sources_mismatch')


def prompt(task, paths):
    if not paths:
        return task
    prefix = ('本轮为只读材料任务。执行目录由系统独立分配；以下路径只用于读取，'
              '不作为工作目录或额外可写目录。不得请求提高写入权限；需要修改、构建或测试时'
              '结束本轮并说明应另开任务工作树。资料可能是当前工作副本，不能自行称为固定快照。\n'
              '允许读取的材料路径：\n' + '\n'.join('- ' + json.dumps(p, ensure_ascii=False) for p in paths) +
              '\n\n原任务：\n')
    result = prefix.encode('utf-8') + task
    if len(result) > 2 * 1024 * 1024:
        raise ValueError('invalid_task_size')
    return result


def exported_prompt(dest, task, value):
    if not record(value):
        return task
    data = (dest / 'prompt.md').read_bytes()
    policy = (dest / 'read-sources.patch.yml').read_bytes()
    if (digest(task) != value.get('task_sha256') or
            digest(data) != value.get('prompt_sha256') or
            data != prompt(task, value['read_sources']) or
            digest(policy) != value['read_policy_sha256']):
        raise ValueError('artifact_modified')
    return data


def native_read_only(rows):
    """A source session must retain the permission state pinned at creation."""
    expected = {'permission/preset': ('preset', 'read-only'),
                'sandbox/mode': ('mode', 'read-only'),
                'approval/policy': ('policy', 'never')}
    seen = set()
    for row in rows:
        kind = row.get('type')
        if kind in expected:
            field, value = expected[kind]
            if row.get('data', {}).get(field) != value:
                raise ValueError('source_permission_changed')
            seen.add(kind)
    if seen != set(expected):
        raise ValueError('source_permission_unverified')
