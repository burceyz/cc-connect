"""Antigravity 1.3.3 protocol and workspace-write process boundary."""
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import tempfile

MODEL = 'gemini-3.8-flash-high'
EFFORT = 'high'
MAX_OUTPUT = 16 * 1024 * 1024
ID_PATTERN = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}')
MCP_TOOLS = ('list_peers', 'send_message', 'read_message', 'ack_message',
             'message_status', 'wait_for_message')


def system_temp_roots():
    roots = [Path(tempfile.gettempdir()), Path('/tmp')]
    return sorted({path.resolve() for path in roots if path.is_dir()})


def sandbox_profile(cwd, temp_roots=None):
    if sys.platform != 'darwin':
        raise ValueError('workspace_sandbox_requires_macos')
    roots = [Path(cwd).resolve(), *(temp_roots or system_temp_roots())]
    if any(not p.is_dir() or p == Path('/') or any(ord(c) < 32 for c in str(p)) for p in roots):
        raise ValueError('invalid_sandbox_root')
    allowed = ' '.join('(subpath ' + json.dumps(str(p), ensure_ascii=False) + ')' for p in roots)
    # Use Hermes's file boundary for the entire CLI, including native tools.
    # The CLI allocates its terminal inside this boundary, so permit the PTY
    # devices as in macOS application.sb; this grants no other file writes.
    return ('(version 1)\n(allow default)\n(deny file-write*)\n'
            '(allow file-write* (literal "/dev/null"))\n'
            '(allow file-write* (literal "/dev/ptmx"))\n'
            '(allow file-write* (require-all (regex #"^/dev/ttys[0-9]+$") '
            '(extension "com.apple.sandbox.pty")))\n'
            '(allow file-write* ' + allowed + ')\n')


def command(executable, cwd, native_dir, task, conversation_id=None, timeout=1800):
    base_args = [str(executable),
                 '--gemini_dir=' + str(native_dir), '--output-format', 'stream-json',
                 '--print-timeout=' + str(int(timeout)) + 's', '--model', MODEL,
                 '--effort', EFFORT, '--disable-slash-commands']
    if os.environ.get('DOROTHY_TEST_UNSANDBOXED') == '1':
        argv = base_args
    else:
        argv = ['/usr/bin/sandbox-exec', '-p', sandbox_profile(cwd), '--'] + base_args
    if conversation_id:
        if not isinstance(conversation_id, str) or not ID_PATTERN.fullmatch(conversation_id):
            raise ValueError('invalid_conversation_id')
        argv.extend(['--conversation', conversation_id])
    # The native print API consumes stdin until EOF. The caller closes stdin.
    return argv + ['-p', task]


def settings(cwd):
    writable = [Path(cwd).resolve(), *system_temp_roots()]
    return {'allowNonWorkspaceAccess': True, 'permissions': {
        'allow': ['read_file(*)', 'read_url(*)', 'command(*)', 'unsandboxed(*)'] +
                 ['mcp(agent-mailbox/' + tool + ')' for tool in MCP_TOOLS] +
                 ['write_file(' + str(p) + ')' for p in writable]}}


def private_file(path, content):
    """Create a new regular file; never follow an agent-created link."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'wb') as handle:
        handle.write(content)


def copy_tree(source, target, root=None):
    """Copy closed native state; preserve internal links without following them."""
    source, target = Path(source), Path(target)
    root = source.resolve() if root is None else root
    if source.is_symlink() or not source.is_dir():
        raise ValueError('native_store_invalid')
    target.mkdir(mode=0o700)
    try:
        for entry in source.iterdir():
            mode = entry.lstat().st_mode
            dest = target / entry.name
            if stat.S_ISDIR(mode):
                copy_tree(entry, dest, root)
            elif stat.S_ISREG(mode):
                # O_NOFOLLOW also protects a last-moment file-to-link swap.
                fd = os.open(entry, os.O_RDONLY | os.O_NOFOLLOW)
                with os.fdopen(fd, 'rb') as incoming:
                    out_mode = 0o700 if mode & stat.S_IXUSR else 0o600
                    out = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, out_mode)
                    with os.fdopen(out, 'wb') as outgoing:
                        shutil.copyfileobj(incoming, outgoing)
            elif stat.S_ISLNK(mode):
                link = os.readlink(entry)
                if Path(link).is_absolute() or not entry.resolve().is_relative_to(root):
                    raise ValueError('native_store_external_link')
                dest.symlink_to(link)
            else:
                raise ValueError('native_store_nonregular_entry')
    except BaseException:
        shutil.rmtree(target)
        raise


def prepare_store(path, cwd, auth_file, archive=None):
    """Use the same temporary path on every turn; never rewrite native databases."""
    path = Path(path)
    if path.parent.resolve() not in system_temp_roots() or path.name.startswith('dorothy-') is False:
        raise ValueError('native_store_not_task_temp')
    if path.exists() or path.is_symlink():
        raise ValueError('native_store_path_occupied')
    created = False
    try:
        if archive is None:
            fd = os.open(auth_file, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as incoming:
                if not stat.S_ISREG(os.fstat(incoming.fileno()).st_mode):
                    raise ValueError('authentication_file_invalid')
                token = incoming.read(1024 * 1024 + 1)
            if not token or len(token) > 1024 * 1024:
                raise ValueError('authentication_file_invalid')
            path.mkdir(mode=0o700)
            created = True
            (path / 'antigravity-cli').mkdir(mode=0o700)
            (path / 'config').mkdir(mode=0o700)
            (path / 'config/projects').mkdir(mode=0o700)
            private_file(path / 'antigravity-cli/antigravity-oauth-token', token)
        else:
            copy_tree(archive, path)
            created = True
        setting_path = path / 'antigravity-cli/settings.json'
        base_settings = settings(cwd)
        merged_settings = base_settings
        if setting_path.exists() or setting_path.is_symlink():
            if not setting_path.is_file() or setting_path.is_symlink():
                raise ValueError('native_settings_invalid')
            try:
                existing = json.loads(setting_path.read_text(encoding='utf-8'))
            except (json.JSONDecodeError, OSError) as exc:
                raise ValueError('native_settings_invalid') from exc
            if not isinstance(existing, dict):
                raise ValueError('native_settings_invalid')
            merged_settings = dict(existing)
            merged_settings['allowNonWorkspaceAccess'] = base_settings['allowNonWorkspaceAccess']
            merged_settings['permissions'] = base_settings['permissions']
            setting_path.unlink()
        private_file(setting_path, (json.dumps(merged_settings, ensure_ascii=False) + '\n').encode())
        return path
    except BaseException:
        if created:
            shutil.rmtree(path)
        raise


class Stream:
    """Only an identified, completed final reply is a report; progress is separate."""
    def __init__(self, expected_id=None):
        self.expected_id = expected_id
        self.session_id = None
        self.model = None
        self.accepted = False
        self.terminal = None
        self.error = None
        self.partial = []
        self.output_size = 0
        self.last_index = -1
        self.last_kind = None
        self.reply_index = -1
        self.reply = []
        self.reply_done = False
        self.tools = {}

    def feed(self, line):
        if self.error:
            return
        try:
            self._feed(json.loads(line))
        except (ValueError, TypeError, KeyError, AttributeError):
            self.error = 'invalid_native_stream'

    def _feed(self, event):
        if not isinstance(event, dict) or not isinstance(event.get('event'), str) or self.terminal is not None:
            raise ValueError()
        kind = event['event']
        if kind == 'init':
            value = event['init']
            sid = event.get('conversation_id') or value.get('conversation_id')
            if (self.session_id is not None or not isinstance(sid, str) or not ID_PATTERN.fullmatch(sid) or
                    (value.get('conversation_id') and value['conversation_id'] != sid) or
                    (self.expected_id and sid != self.expected_id)):
                raise ValueError()
            self.session_id, self.model = sid, value.get('model')
            if self.model != MODEL:
                self.error = 'native_model_mismatch'
        elif kind == 'step_update':
            self._step(event['step_update'])
        elif kind == 'result':
            value = event['result']
            if not isinstance(value, dict) or value.get('conversation_id') != self.session_id or not self.session_id:
                raise ValueError()
            self.terminal = {k: value.get(k) for k in ('status', 'usage')}
            if value.get('status') != 'SUCCESS':
                self.error = 'native_run_failed'

    def _step(self, step):
        if not self.session_id or step.get('conversation_id') != self.session_id:
            raise ValueError()
        index, kind = step.get('step_index'), step.get('step_type')
        if type(index) is not int or index < 0:
            raise ValueError()
        state = step.get('state')
        if kind in ('checkpoint', 'system_message', 'finish'):
            return
        if index >= self.last_index:
            self.last_index, self.last_kind = index, kind
        if kind == 'user_input':
            self.accepted = self.accepted or state == 'DONE'
        elif kind == 'agent_response':
            text = step.get('text_delta', '')
            if not isinstance(text, str) or state not in ('ACTIVE', 'DONE'):
                raise ValueError()
            self.output_size += len(text.encode('utf-8'))
            if self.output_size > MAX_OUTPUT:
                self.error = 'native_output_too_large'
                return
            self.partial.append(text)
            if index > self.reply_index:
                self.reply_index, self.reply, self.reply_done = index, [], False
            elif index < self.reply_index or self.reply_done:
                raise ValueError()
            self.reply.append(text)
            self.reply_done = state == 'DONE'
        elif kind == 'tool':
            info = step.get('tool_info') or {}
            self.tools[index] = {'name': step.get('tool_name') or info.get('name'),
                                 'state': state, 'error': bool(info.get('error'))}

    def report(self):
        text = ''.join(self.reply)
        if (not self.error and self.terminal and self.accepted and self.reply_done and text.strip() and
                self.reply_index == self.last_index and self.last_kind == 'agent_response'):
            return text
        return None
