#!/usr/bin/env python3
"""One bounded DingTalk notification per async job; credentials stay local."""
from datetime import datetime, timedelta, timezone
import fcntl
import json
from pathlib import Path
import re
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from urllib.parse import urlsplit

import bridge

CONFIG = 'dingtalk.private.json'
DEADLINE = 15
MEMBER_LABEL = 'Hermes（DSH）'
OWNER_LABEL = 'Atlas（Codex）'


def load_config(runtime):
    path = runtime / CONFIG
    if not path.exists():
        return None
    if path.is_symlink() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise ValueError('private_config_permissions')
    value = json.loads(path.read_text(encoding='utf-8'))
    if value.get('enabled') is False:
        return None
    if value.get('enabled') is not True or not re.fullmatch(
            r'https://oapi\.dingtalk\.com/robot/send\?access_token=[A-Za-z0-9]{1,256}',
            value.get('webhook', '')):
        raise ValueError('invalid_notification_config')
    proxy = value.get('proxy')
    if proxy:
        parsed = urlsplit(proxy)
        if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or
                parsed.username or parsed.password or parsed.query or parsed.fragment or
                parsed.path not in {'', '/'}):
            raise ValueError('invalid_notification_proxy')
    return value


def validate_title(title):
    if (not isinstance(title, str) or len(title) > 80 or
            any(ord(c) < 32 or ord(c) == 127 for c in title) or
            re.search(r'https?://|access_token', title, re.I)):
        raise ValueError('invalid_notification_title')
    return title.strip()


def message(record):
    labels = {'completed': '本轮执行已结束', 'timeout': '已超时', 'interrupted': '已中断',
              'launch_failed': '启动失败'}
    label = labels.get(record['status'], '未完成')
    ended = datetime.fromisoformat(record['ended']).astimezone(
        timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S +08:00')
    title = validate_title(record.get('title', '')) or record['job_id']
    detail = ('结果已保存在工作区；是否完成需求及提交推送情况，以交付报告为准。' if record['status'] == 'completed'
              else '本次任务未成功完成，可查看状态记录；未自动重跑。')
    delivery = ('交付方式：会话接续；是否送达原会话，以IPC回执为准。'
                if record.get('delivery') == 'conversation'
                else '交付方式：通知结束；无需Atlas自动收尾。')
    return '\n'.join([
        f'任务进度｜{MEMBER_LABEL}｜{label}', f'任务：{title}', f'编号：{record["job_id"]}',
        f'结束时间：{ended}', detail, delivery,
        f'按需查看：对 {OWNER_LABEL} 说“查 DSH 结果 {record["job_id"]}”。'])


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def post(config, content):
    """Never return provider bodies, URLs or raw exceptions (may contain tokens)."""
    proxy = config.get('proxy')
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({'http': proxy, 'https': proxy} if proxy else {}),
        NoRedirect())
    payload = {'msgtype': 'text', 'text': {'content': content}}
    request = urllib.request.Request(
        config['webhook'], data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers={'Content-Type': 'application/json; charset=utf-8'}, method='POST')
    try:
        with opener.open(request, timeout=8) as response:
            if response.status != 200:
                return {'status': 'unknown', 'code': 'unexpected_http_status'}
            raw = response.read(8193)
            if len(raw) > 8192:
                return {'status': 'unknown', 'code': 'oversized_response'}
            value = json.loads(raw)
        code = value.get('errcode') if isinstance(value, dict) else None
        if type(code) is not int:
            return {'status': 'unknown', 'code': 'invalid_response'}
        if code == 0:
            return {'status': 'sent', 'errcode': 0}
        return {'status': 'failed', 'code': 'dingtalk_rejected', 'errcode': code}
    except urllib.error.HTTPError as exc:
        code = exc.code
        exc.close()
        return {'status': 'failed' if 300 <= code < 500 else 'unknown',
                'code': 'http_error', 'http_status': code}
    except Exception:
        # A lost response does not establish whether the server accepted the POST.
        return {'status': 'unknown', 'code': 'transport_or_response_error'}


def send(config, content):
    # Enforce a wall-clock limit as socket timeouts alone do not bound DNS/read time.
    # Secret is carried on stdin, never in process arguments or subprocess output.
    try:
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), '_send'],
            input=json.dumps({'config': config, 'content': content}), text=True,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=DEADLINE)
        value = json.loads(proc.stdout)
        if proc.returncode or value.get('status') not in {'sent', 'failed', 'unknown'}:
            raise ValueError('invalid_sender_result')
        return value
    except (Exception, KeyboardInterrupt):
        return {'status': 'unknown', 'code': 'sender_interrupted_or_timed_out'}


def read(dest):
    path = dest / 'notification.json'
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding='utf-8'))
    if value['status'] == 'sending':
        with (dest / 'notification.lock').open('a') as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                value = json.loads(path.read_text(encoding='utf-8'))
                if value['status'] == 'sending':
                    value = value | {'status': 'unknown', 'code': 'sender_stopped'}
    return value


def once(dest, record, runtime):
    if not record.get('notification_requested') or record['status'] in {'accepted', 'running'}:
        return None
    with (dest / 'notification.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {'status': 'sending'}
        if (dest / 'notification.json').exists():
            value = json.loads((dest / 'notification.json').read_text(encoding='utf-8'))
            if value['status'] == 'sending':
                value = value | {'status': 'unknown', 'code': 'sender_stopped'}
            return value
        value = {'channel': 'dingtalk', 'attempted': bridge.now()}
        try:
            config = load_config(runtime)
            content = message(record)
        except Exception:
            value.update(status='failed', code='notification_config_or_template_invalid')
        else:
            if config is None:
                value.update(status='skipped', code='notification_disabled_or_missing')
            else:
                # Persist the attempt BEFORE sending. An uncertain send is never retried.
                value.update(status='sending')
                bridge.write_json(dest / 'notification.json', value)
                value.update(send(config, content))
        value['ended'] = bridge.now()
        bridge.write_json(dest / 'notification.json', value)
        return value


def finish(dest, record, runtime):
    """Best effort after the task receipt is durable; never rewrite the task status."""
    try:
        return once(dest, record, runtime)
    except (Exception, KeyboardInterrupt):
        return {'status': 'unknown', 'code': 'notification_not_recorded'}


if __name__ == '__main__':
    if sys.argv[1:] != ['_send']:
        sys.exit(2)
    try:
        args = json.load(sys.stdin)
        print(json.dumps(post(args['config'], args['content'])))
    except Exception:
        print(json.dumps({'status': 'unknown', 'code': 'sender_error'}))
