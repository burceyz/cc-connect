#!/usr/bin/env python3
"""Command-line front end for one agent-mailbox binding.

Machine output is JSON on stdout. Errors go to stderr with a plain-language
explanation; the exit code is non-zero.
"""

import argparse
import json
import sys

from client import (
    MailboxError,
    MailboxTransportError,
    client_for,
    hint_for,
    load_binding,
)


def build_parser():
    parser = argparse.ArgumentParser(prog="mailbox.py", description="agent-mailbox v1 client")
    parser.add_argument("--binding", help="path to the private binding JSON")
    parser.add_argument("--timeout", type=float, default=12, help="request timeout in seconds")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="check the binding, service health, and own mailbox")
    sub.add_parser("peers", help="list members you may talk to")

    send = sub.add_parser("send", help="send a message from a UTF-8 file")
    send.add_argument("--to", required=True, help="recipient mailbox_id")
    send.add_argument("--id", dest="message_id", required=True, help="stable client_message_id")
    send.add_argument("--task-id", required=True, help="task association")
    send.add_argument("--file", required=True, help="path to a UTF-8 body file")
    send.add_argument("--reply-to", default="", help="optional original message_id")

    show = sub.add_parser("show", help="read a message and its state")
    show.add_argument("--id", dest="message_id", required=True)

    ack = sub.add_parser("ack", help="confirm you read a message addressed to you")
    ack.add_argument("--id", dest="message_id", required=True)

    sub.add_parser("mcp", help="run the MCP stdio server for this binding")
    return parser


def binding_summary(binding):
    return {
        "socket": binding.get("socket", ""),
        "mailbox_id": binding.get("mailbox_id", ""),
        "generation": binding.get("generation", ""),
    }


def run_command(client, args, binding):
    if args.command == "doctor":
        return {"binding": binding_summary(binding), "health": client.health(), "me": client.me()}
    if args.command == "peers":
        return {"peers": client.peers()}
    if args.command == "send":
        with open(args.file, "rb") as handle:
            body = handle.read().decode("utf-8")
        return {
            "message": client.send(
                {
                    "client_message_id": args.message_id,
                    "to": args.to,
                    "body": body,
                    "task_id": args.task_id,
                    "reply_to": args.reply_to,
                }
            )
        }
    if args.command == "show":
        return {"message": client.get(args.message_id)}
    if args.command == "ack":
        return {"message": client.ack(args.message_id)}
    raise ValueError("unknown command: %s" % args.command)


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        binding = load_binding(args.binding)
    except ValueError as exc:
        return _fail(2, str(exc))

    if args.command == "mcp":
        from mcp_server import serve

        serve(binding)
        return 0

    try:
        client = client_for(binding, timeout=args.timeout)
        payload = run_command(client, args, binding)
    except ValueError as exc:
        return _fail(2, str(exc))
    except MailboxError as exc:
        return _fail(1, "%s：%s" % (exc.code, hint_for(exc.code)))
    except MailboxTransportError as exc:
        return _fail(2, str(exc))
    except (OSError, UnicodeDecodeError) as exc:
        return _fail(2, "cannot read body file: %s" % exc)

    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


def _fail(code, message):
    sys.stderr.write("mailbox.py: %s\n" % message)
    return code


if __name__ == "__main__":
    sys.exit(main())
