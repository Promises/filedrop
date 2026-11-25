#!/usr/bin/env python3
"""filedrop: one-time links to receive one large file from someone, or send one to them.

Receiving: `filedrop new` makes an upload link. The sender (macOS or Linux, needs only curl) runs:

    curl -fsSL https://drop.example.com/u/<token> | sh -s -- /path/to/file

That fetches a small POSIX sh uploader which PUTs the file with curl and, after any
interruption, asks for the received offset (HEAD) and continues with `curl -C <offset> -T`
(curl then sends Content-Range). Partial data lives in <dir>/.partial; when the last byte
arrives the file is moved into its own folder <dir>/<date>_<note>_<id>/ and the link is spent.

Sending: `filedrop share <path>` makes a download link for a file on this server. The link
opens a small page with a Download button and a `curl -fC - -o <name> <file URL>` command;
both resume with HTTP Range requests. Only the file URL serves bytes, so chat apps that
fetch the link for a preview can't use up a one-time download. The link is spent once the last byte has been sent; for DOWNLOAD_GRACE
afterwards it still answers requests that resume a download, but not new full downloads.
The file is served in place and must not change while the link is open.

A link's deadline only limits when the transfer can start. Once it has started, the link
stays open until the transfer is complete or the link is revoked, however long that takes;
an abandoned upload keeps its partial data until `filedrop revoke`.

Commands:
    filedrop serve                                      run the HTTP service (behind a TLS proxy)
    filedrop new [--hours H] [--max SIZE] [--note TEXT]  create an upload link and print the command
    filedrop share PATH [--hours H] [--note TEXT]       create a download link for a file
    filedrop list                                       show links, with progress of transfers in flight
    filedrop watch [--interval S]                       follow transfers in flight: progress, speed, time left
    filedrop revoke <id>                                kill a link (and drop an upload's partial data)

Environment:
    FILEDROP_PUBLIC_URL  base URL people use, e.g. https://drop.example.com (required for serve, new, share)
    FILEDROP_DIR         received files go in a folder per link here (default /srv/filedrop)
    FILEDROP_MOUNT       if set, refuse uploads unless this path is a mount point, so a missing
                         network or USB mount can never fill the local disk (default: no check)
    FILEDROP_MIN_FREE    never let an upload take free space below this (default 10G)
    FILEDROP_KEEP_DAYS   forget finished, revoked and expired links after this many days;
                         their files are not touched (default 30, 0 keeps them forever)
    FILEDROP_STATE       token database (default /var/lib/filedrop/tokens.json)
    FILEDROP_LISTEN      host:port to listen on (default 127.0.0.1:8099)
"""

import argparse
import contextlib
import fcntl
import hashlib
import html
import json
import os
import re
import secrets
import shlex
import shutil
import sys
import threading
import time
from datetime import datetime
from string import Template
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, unquote


def parse_size(text):
    """Parse sizes like 500G, 1.5T, 200MB or 1024 into bytes (binary units)."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMGT]?)B?", text.strip().upper())
    if not m:
        raise argparse.ArgumentTypeError(f"bad size: {text}")
    return int(float(m.group(1)) * 1024 ** " KMGT".index(m.group(2) or " "))


PUBLIC_URL = os.environ.get("FILEDROP_PUBLIC_URL", "").rstrip("/")
DROP_DIR = os.environ.get("FILEDROP_DIR", "/srv/filedrop")
MOUNT = os.environ.get("FILEDROP_MOUNT", "")
MIN_FREE = parse_size(os.environ.get("FILEDROP_MIN_FREE", "10G"))
KEEP_DAYS = float(os.environ.get("FILEDROP_KEEP_DAYS", "30"))
STATE = os.environ.get("FILEDROP_STATE", "/var/lib/filedrop/tokens.json")
LISTEN = os.environ.get("FILEDROP_LISTEN", "127.0.0.1:8099")

PARTIAL_DIR = os.path.join(DROP_DIR, ".partial")
CHUNK = 1 << 20
SEND_CHUNK = 8 << 20
HOUSEKEEPING_INTERVAL = 3600
ACTIVE_WINDOW = 30  # a transfer that moved data within this many seconds counts as running now
PROGRESS_SAVE_INTERVAL = 5  # how often a download records its progress for `list` and `watch`
DOWNLOAD_GRACE = 600  # after the last byte went out, resuming requests are still answered this long
TOKEN_RE = re.compile(r"^/([ud])/([A-Za-z0-9_-]{20,64})(?:/(.*))?$")
CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")
RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")
UNSATISFIABLE = "unsatisfiable"
KINDS = {"u": "upload", "d": "download"}

UPLOADER = r"""#!/bin/sh
# filedrop uploader: sends one file to @URL@ and resumes after interruptions.
# Usage: curl -fsSL @URL@ | sh -s -- /path/to/file
set -u
file=${1:-}
if [ -z "$file" ] || [ ! -f "$file" ]; then
	echo "usage: curl -fsSL @URL@ | sh -s -- /path/to/file" >&2
	exit 2
fi
size=$(wc -c < "$file" | tr -d ' ')
name=$(basename "$file")
url="@URL@/$(printf '%s' "$name" | od -An -v -tx1 | tr -d ' \n' | sed 's/../%&/g')"
tmp=$(mktemp)
body=$(mktemp)
trap 'rm -f "$tmp" "$body"' EXIT
tries=0
last=-1

human() {
	awk -v b="$1" 'BEGIN { split("B KB MB GB TB", u); i = 1; while (b >= 1024 && i < 5) { b /= 1024; i++ }
		printf(i == 1 ? "%d %s" : "%.1f %s", b, u[i]) }'
}

echo "Sending $name ($(human "$size")) to @HOST@."
echo "Safe to interrupt: run the same command again and it continues where it stopped."
echo "Progress, from anywhere: @URL@/status"

retry() {
	tries=$((tries + 1))
	if [ "$tries" -gt 60 ]; then
		echo "Giving up after $tries attempts. Run the same command again later to resume." >&2
		exit 1
	fi
	echo "$1, retrying in 10 s" >&2
	sleep 10
}

while :; do
	code=$(curl -sS -I -o "$tmp" -w '%{http_code}' "$url") || code=000
	headers=$(tr -d '\r' < "$tmp")
	case $code in
	200) ;;
	000 | 5??) retry "Server unreachable"; continue ;;
	*)
		echo "Upload refused: $(printf '%s\n' "$headers" | sed -n 's/^[Xx]-[Ff]iledrop-[Mm]essage: *//p')" >&2
		exit 1
		;;
	esac
	if printf '%s\n' "$headers" | grep -qi '^upload-complete: *1'; then
		echo "$name has been received."
		exit 0
	fi
	offset=$(printf '%s\n' "$headers" | sed -n 's/^[Uu]pload-[Oo]ffset: *//p')
	offset=${offset:-0}
	if [ "$offset" -gt "$last" ]; then tries=0; fi
	last=$offset
	if [ "$offset" -gt 0 ]; then
		echo "Already received $(human "$offset") ($(awk -v o="$offset" -v s="$size" 'BEGIN { printf("%d", o * 100 / s) }')%), sending the rest." >&2
	fi
	# -C also makes curl append to the -o file, so the response gets its own emptied file
	: > "$body"
	# curl's meter (stderr) shows this attempt's bytes, speed and time left
	code=$(curl -C "$offset" -T "$file" -o "$body" -w '%{http_code}' "$url") || code=000
	case $code in
	200 | 201) cat "$body"; exit 0 ;;
	416 | 423) retry "Previous attempt still settling" ;;
	000 | 408 | 5??) retry "Connection lost" ;;
	*) cat "$body" >&2; exit 1 ;;
	esac
done
"""


LANDING = Template("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${name}</title>
<style>
:root { color-scheme: light dark; --fg: #1d1d1f; --muted: #6e6e73; --bg: #f5f5f7; --card: #fff; --accent: #0a64d6; }
@media (prefers-color-scheme: dark) {
  :root { --fg: #f5f5f7; --muted: #a1a1a6; --bg: #1d1d1f; --card: #2c2c2e; --accent: #4a9eff; }
}
body { margin: 0; padding: 12vh 16px; background: var(--bg); color: var(--fg); font: 16px/1.5 system-ui, sans-serif; }
.card { box-sizing: border-box; max-width: 34rem; margin: 0 auto; padding: 2rem; border-radius: 12px;
        background: var(--card); }
.name { margin: 0; font-size: 1.25rem; overflow-wrap: anywhere; }
.size { margin: .25rem 0 1.5rem; color: var(--muted); }
.download { display: inline-block; padding: .75rem 1.5rem; border-radius: 8px; background: var(--accent);
            color: #fff; font-weight: 600; text-decoration: none; }
.terminal { margin-top: 1.5rem; color: var(--muted); font-size: .875rem; }
.terminal pre { margin: .5rem 0 0; padding: .75rem; border-radius: 8px; background: var(--bg); color: var(--fg);
                 white-space: pre-wrap; overflow-wrap: anywhere; }
.note { margin: 1.5rem 0 0; color: var(--muted); font-size: .875rem; }
</style>
</head>
<body>
<main class="card">
<h1 class="name">${name}</h1>
<p class="size">${size}</p>
<a class="download" href="${url}" download>Download</a>
<div class="terminal">Or in a terminal (run it again to resume if the connection drops):<pre>${curl}</pre></div>
<p class="note">This link works once: when the whole file has been downloaded it stops working.</p>
</main>
</body>
</html>
""")


def token_key(token):
    return hashlib.sha256(token.encode()).hexdigest()


@contextlib.contextmanager
def tokens():
    """Yield the token database under an exclusive lock; write it back if it changed."""
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            with open(STATE) as f:
                db = json.load(f)
        except FileNotFoundError:
            db = {}
        before = json.dumps(db, sort_keys=True)
        yield db
        if json.dumps(db, sort_keys=True) != before:
            tmp = STATE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(db, f, indent=2, sort_keys=True)
            os.chmod(tmp, 0o600)
            os.replace(tmp, STATE)


def kind_of(entry):
    return entry.get("kind", "upload")  # links made before downloads existed have no kind


def is_expired(entry):
    """A transfer that has started never expires; only a link nobody has used yet does."""
    return entry["state"] == "expired" or (entry["state"] == "open" and time.time() > entry["expires"])


def usable(db, token, kind, allow_done=False):
    """Return (entry, None) for a link that may be used, else (None, (status, message))."""
    entry = db.get(token_key(token))
    if entry is None or kind_of(entry) != kind:
        return None, (404, "Unknown link.")
    if entry["state"] == "done" and allow_done:
        return entry, None
    if entry["state"] == "done":
        return None, (410, "This link has already been used.")
    if entry["state"] == "revoked":
        return None, (410, "This link has been revoked.")
    if is_expired(entry):
        return None, (410, "This link has expired.")
    return entry, None


def partial_path(entry):
    return os.path.join(PARTIAL_DIR, entry["id"])


def clean_name(raw):
    name = unquote(raw).replace("\\", "/").split("/")[-1]
    name = "".join(c for c in name if c.isprintable()).strip().lstrip(".")
    while len(name.encode()) > 200:
        name = name[:-1]
    return name or "upload.bin"


def link_dir(entry):
    """Folder for one link's file: <date>_<note>_<id>, so senders never collide."""
    note = re.sub(r"[^\w.-]+", "-", entry["note"]).strip("-.")[:40]
    date = datetime.fromtimestamp(entry["created"]).strftime("%Y-%m-%d")
    return os.path.join(DROP_DIR, "_".join(p for p in (date, note, entry["id"]) if p))


def unique_path(directory, name):
    stem, ext = os.path.splitext(name)
    path, n = os.path.join(directory, name), 2
    while os.path.exists(path):
        path, n = os.path.join(directory, f"{stem} ({n}){ext}"), n + 1
    return path


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def etag(entry):
    return f'"{entry["total"]:x}-{entry["mtime_ns"]:x}"'


def parse_range(header, if_range, current_etag, size):
    """Interpret a Range request for one range.

    Returns None to send the whole file (no Range, a stale If-Range, several ranges or
    anything unparsable), UNSATISFIABLE when the range starts past the end, else (start, end).
    """
    if not header or (if_range and if_range.strip() != current_etag):
        return None
    m = RANGE_RE.fullmatch(header.strip())
    if not m or not (m.group(1) or m.group(2)):
        return None
    if m.group(1):
        start = int(m.group(1))
        end = min(int(m.group(2)), size - 1) if m.group(2) else size - 1
        if start >= size:
            return UNSATISFIABLE
        if end < start:
            return None
    else:
        suffix = int(m.group(2))
        if suffix == 0:
            return UNSATISFIABLE
        start, end = max(size - suffix, 0), size - 1
    return start, end


def content_disposition(name):
    """attachment header with an ASCII fallback name and the real name in RFC 5987 form."""
    fallback = "".join(c if 32 <= ord(c) < 127 and c not in '"\\' else "_" for c in name)
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(name, safe='')}"


def ended_at(entry):
    """When a finished, revoked or expired link stopped being usable."""
    return entry.get("ended") or entry.get("finished") or entry["expires"]


def housekeeping():
    """Expire unused links past their deadline and forget old finished ones.

    Transfers in progress are never expired. Only token records are forgotten; files
    stay where they are.
    """
    now = time.time()
    with tokens() as db:
        for key, entry in list(db.items()):
            if entry["state"] == "open" and is_expired(entry):
                entry.update(state="expired", ended=now)
                log(f"{entry['id']} expired")
            elif (KEEP_DAYS > 0 and entry["state"] in ("done", "revoked", "expired")
                  and now - ended_at(entry) > KEEP_DAYS * 86400):
                del db[key]
                log(f"{entry['id']} forgotten ({entry['state']} more than {KEEP_DAYS:g} days ago)")


def log(message):
    print(message, file=sys.stderr, flush=True)


_upload_locks = {}
_upload_locks_guard = threading.Lock()


def upload_lock(key):
    with _upload_locks_guard:
        return _upload_locks.setdefault(key, threading.Lock())


class Handler(BaseHTTPRequestHandler):
    server_version = "filedrop"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    timeout = 300  # a stalled peer frees its connection (and an upload its lock) after this

    def log_request(self, code="-", size="-"):
        path = TOKEN_RE.sub(lambda m: f"/{m.group(1)}/{token_key(m.group(2))[:8]}/{m.group(3) or ''}",
                            self.path.split("?", 1)[0])
        self.log_message('"%s %s" %s', self.command, path, code)

    def log_message(self, fmt, *args):
        log(f"{self.headers.get('X-Forwarded-For', self.client_address[0])} " + fmt % args)

    def reply(self, code, text="", headers=None, content_type="text/plain; charset=utf-8"):
        body = (text + "\n" if text else "").encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        if code >= 400 and text:
            self.send_header("X-Filedrop-Message", text.encode("ascii", "replace").decode())
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self.close_connection = True

    def route(self):
        """Return (kind, token, rest of the path) for /u/... and /d/... links, else (None, None, None)."""
        m = TOKEN_RE.match(self.path.split("?", 1)[0])
        if not m:
            return None, None, None
        return KINDS[m.group(1)], m.group(2), m.group(3)

    def upload_target(self):
        """Return (token, file name) for /u/<token>/<name>, or reply 404 and return None."""
        kind, token, rest = self.route()
        if kind != "upload" or not rest:
            self.reply(404, "Not found.")
            return None
        return token, clean_name(rest)

    def do_GET(self):
        kind, token, rest = self.route()
        if kind == "download":
            return self.download(token) if rest else self.landing(token)
        if kind == "upload" and rest == "status":
            return self.status(token)
        if kind != "upload" or rest:
            return self.reply(404, "Not found.")
        with tokens() as db:
            _, error = usable(db, token, "upload")
        if error:
            return self.reply(*error)
        script = UPLOADER.replace("@URL@", f"{PUBLIC_URL}/u/{token}")
        script = script.replace("@HOST@", PUBLIC_URL.split("://", 1)[-1])
        self.reply(200, script.rstrip("\n"), content_type="text/x-shellscript; charset=utf-8")

    def status(self, token):
        """Upload progress for the sender, readable in a browser or with curl."""
        with tokens() as db:
            entry, error = usable(db, token, "upload", allow_done=True)
        if error:
            return self.reply(*error)
        if entry["state"] == "done":
            text = f"{entry['filename']} ({human(entry['total'])}) has been received. Thanks!"
        elif entry["state"] == "receiving":
            got, idle = progress(entry)
            pct = got * 100 / entry["total"] if entry["total"] else 100
            activity = ("uploading now" if idle is not None and idle < ACTIVE_WINDOW
                        else "paused - run the upload command again to continue")
            text = f"{entry['filename']}: {human(got)} of {human(entry['total'])} received ({pct:.0f}%), {activity}."
        else:
            until = datetime.fromtimestamp(entry["expires"]).strftime("%Y-%m-%d %H:%M")
            text = f"Nothing received yet. Start the upload before {until}; once started it can take as long as it needs."
        self.reply(200, text)

    def do_HEAD(self):
        kind, token, rest = self.route()
        if kind == "download":
            return self.download(token) if rest else self.landing(token)
        target = self.upload_target()
        if target is None:
            return
        token, name = target
        with tokens() as db:
            entry, error = usable(db, token, "upload", allow_done=True)
        if error:
            return self.reply(*error)
        if entry["filename"] and entry["filename"] != name:
            return self.reply(409, f"This link is already in use for {entry['filename']}.")
        if entry["state"] == "done":
            return self.reply(200, headers={"Upload-Offset": str(entry["total"]), "Upload-Complete": "1"})
        try:
            offset = os.path.getsize(partial_path(entry))
        except FileNotFoundError:
            offset = 0
        self.reply(200, headers={"Upload-Offset": str(offset)})

    def do_PUT(self):
        target = self.upload_target()
        if target is None:
            return
        token, name = target
        length = self.headers.get("Content-Length", "")
        if not length.isdigit():
            return self.reply(411, "Content-Length required: upload a regular file, not a pipe.")
        length = int(length)
        start, total = 0, length
        content_range = self.headers.get("Content-Range")
        if content_range:
            m = CONTENT_RANGE_RE.fullmatch(content_range.strip())
            if not m:
                return self.reply(400, "Bad Content-Range.")
            start, end, total = map(int, m.groups())
            if end - start + 1 != length or end >= total:
                return self.reply(400, "Content-Range does not match Content-Length.")
        if MOUNT and not os.path.ismount(MOUNT):
            return self.reply(503, "Storage is offline, try again later.")
        lock = upload_lock(token_key(token))
        if not lock.acquire(blocking=False):
            # 423, not 409: the uploader retries this one, while 409s are permanent
            return self.reply(423, "An upload with this link is already running.")
        try:
            self.receive(token, name, start, length, total)
        finally:
            lock.release()

    def receive(self, token, name, start, length, total):
        with tokens() as db:
            entry, error = usable(db, token, "upload")
            if error:
                return self.reply(*error)
            if entry["filename"] and entry["filename"] != name:
                return self.reply(409, f"This link is already in use for {entry['filename']}.")
            if start > 0 and entry["total"] != total:
                return self.reply(409, "The file changed size; restart the upload from the beginning.")
            if total > entry["max_bytes"]:
                return self.reply(413, f"File is larger than this link allows ({human(entry['max_bytes'])}).")
            os.makedirs(PARTIAL_DIR, exist_ok=True)
            if shutil.disk_usage(PARTIAL_DIR).free - MIN_FREE < total - start:
                return self.reply(507, "Not enough free space on the receiving side.")
            part = partial_path(entry)
            have = os.path.getsize(part) if os.path.exists(part) else 0
            if start > 0 and start != have:
                return self.reply(416, f"Expected offset {have}.", headers={"Upload-Offset": str(have)})
            entry.update(filename=name, total=total, state="receiving")
            entry_id = entry["id"]

        received = 0
        try:
            with open(part, "r+b" if start else "wb") as f:
                f.seek(start)
                while received < length:
                    chunk = self.rfile.read(min(CHUNK, length - received))
                    if not chunk:
                        break
                    f.write(chunk)
                    received += len(chunk)
                if start + received == total:
                    f.flush()
                    os.fsync(f.fileno())
        except OSError as e:
            log(f"{entry_id} {name}: interrupted at {start + received}/{total}: {e}")
            return
        if start + received < total:
            log(f"{entry_id} {name}: paused at {start + received}/{total}")
            return

        with tokens() as db:
            entry = db[token_key(token)]
            if entry["state"] != "receiving":
                # revoked mid-upload: revoke already unlinked the partial file
                with contextlib.suppress(FileNotFoundError):
                    os.remove(part)
                return self.reply(410, "This link has been revoked.")
            directory = link_dir(entry)
            os.makedirs(directory, exist_ok=True)
            dest = unique_path(directory, name)
            os.replace(part, dest)
            os.chmod(dest, 0o664)
            now = time.time()
            entry.update(state="done", path=dest, finished=now, ended=now)
        log(f"{entry_id} {name}: complete, {human(total)} -> {dest}")
        self.reply(201, f"Received {name} ({human(total)}). Thanks!")

    def landing(self, token):
        """The page a download link opens: file name, size, a Download button and the curl command."""
        with tokens() as db:
            entry, error = usable(db, token, "download")
        if error:
            return self.reply(*error)
        url = download_url(token, entry["filename"])
        page = LANDING.substitute(
            name=html.escape(entry["filename"]), size=html.escape(human(entry["total"])),
            url=html.escape(url), curl=html.escape(curl_command(entry["filename"], url)))
        self.reply(200, page, content_type="text/html; charset=utf-8", headers={
            "Cache-Control": "no-store",
            "Referrer-Policy": "no-referrer",
            "X-Robots-Tag": "noindex",
            "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'",
        })

    def download(self, token):
        """Serve a shared file (GET or HEAD), with Range support so downloads can resume."""
        with tokens() as db:
            entry, error = usable(db, token, "download", allow_done=True)
            if error:
                return self.reply(*error)
            size = entry["total"]
            rng = parse_range(self.headers.get("Range"), self.headers.get("If-Range"), etag(entry), size)
            resuming = rng == UNSATISFIABLE or (rng is not None and rng[0] > 0)
            if entry["state"] == "done" and not (resuming and time.time() - entry["finished"] < DOWNLOAD_GRACE):
                return self.reply(410, "This link has already been used.")
            try:
                f = open(entry["source"], "rb")
            except OSError as e:
                log(f"{entry['id']} cannot read {entry['source']}: {e}")
                return self.reply(410, "This file is no longer available.")
            st = os.fstat(f.fileno())
            if (st.st_size, st.st_mtime_ns) != (size, entry["mtime_ns"]):
                f.close()
                log(f"{entry['id']} {entry['source']} changed since the link was made")
                return self.reply(410, "This file has changed since the link was made.")
            if self.command == "GET" and entry["state"] == "open":
                entry.update(state="sending", active=time.time())
            entry = dict(entry)

        with f:
            if rng == UNSATISFIABLE:
                return self.reply(416, "Requested range not satisfiable.", headers={"Content-Range": f"bytes */{size}"})
            start, end = rng or (0, size - 1)
            length = end - start + 1
            self.send_response(206 if rng else 200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Disposition", content_disposition(entry["filename"]))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", etag(entry))
            self.send_header("Cache-Control", "no-store")
            self.send_header("Connection", "close")
            if rng:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            self.close_connection = True
            if self.command == "HEAD":
                return
            sent = self.send_file(f, start, length, token, entry)
        if sent == length and end == size - 1:
            self.delivered(token)

    def send_file(self, f, offset, count, token, entry):
        """Send count bytes from offset; return how many went out."""
        sent, saved = 0, time.time()
        try:
            while sent < count:
                n = self.connection.sendfile(f, offset + sent, min(SEND_CHUNK, count - sent))
                if not n:
                    break
                sent += n
                if time.time() - saved >= PROGRESS_SAVE_INTERVAL:
                    saved = time.time()
                    if record_delivery(token, offset + sent) == "revoked":
                        log(f"{entry['id']} {entry['filename']}: revoked during download")
                        break
        except OSError as e:
            log(f"{entry['id']} {entry['filename']}: interrupted at {offset + sent}/{entry['total']}: {e}")
        record_delivery(token, offset + sent)
        return sent

    @staticmethod
    def delivered(token):
        with tokens() as db:
            entry = db.get(token_key(token))
            if entry is None or entry["state"] != "sending":
                return  # already done (a resumed request in the grace period) or revoked
            now = time.time()
            entry.update(state="done", delivered=entry["total"], finished=now, ended=now)
        log(f"{entry['id']} {entry['filename']}: delivered, {human(entry['total'])}")


def record_delivery(token, position):
    """Note how far a download has got (for list and watch); return the link's state."""
    with tokens() as db:
        entry = db.get(token_key(token))
        if entry is None:
            return None
        entry["delivered"] = max(entry.get("delivered") or 0, position)
        entry["active"] = time.time()
        return entry["state"]


def require_public_url():
    if not re.match(r"^https?://[^/]+", PUBLIC_URL):
        sys.exit("Set FILEDROP_PUBLIC_URL to the address people use, e.g. https://drop.example.com")


def make_server(listen=None):
    host, port = (listen or LISTEN).rsplit(":", 1)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    server.daemon_threads = True
    return server


def cmd_serve(_args):
    require_public_url()
    housekeeping()

    def periodic():
        while True:
            time.sleep(HOUSEKEEPING_INTERVAL)
            housekeeping()

    threading.Thread(target=periodic, daemon=True).start()
    server = make_server()
    log(f"filedrop listening on {LISTEN} for {PUBLIC_URL}, files go to {DROP_DIR}")
    server.serve_forever()


def new_entry(kind, hours, note, **fields):
    """Create a link and return its secret token; only the token's hash is stored."""
    token = secrets.token_urlsafe(24)
    key = token_key(token)
    now = time.time()
    with tokens() as db:
        db[key] = {
            "kind": kind, "id": key[:8], "note": note, "created": now, "expires": now + hours * 3600,
            "state": "open", "filename": None, "total": None, "path": None, "finished": None, "ended": None,
            **fields,
        }
    return token


def create_link(hours, max_bytes, note=""):
    """Create an upload link; return its token."""
    return new_entry("upload", hours, note, max_bytes=max_bytes)


def create_share(path, hours, note=""):
    """Create a download link for a file on this server; return its token.

    Raises OSError or ValueError if the file can't be shared.
    """
    source = os.path.realpath(path)
    if not os.path.isfile(source):
        raise ValueError("not a file")
    with open(source, "rb") as f:  # fail now, not when the recipient clicks, if we can't read it
        st = os.fstat(f.fileno())
    return new_entry("download", hours, note, source=source, filename=os.path.basename(source),
                     total=st.st_size, mtime_ns=st.st_mtime_ns, delivered=0, active=None)


def download_url(token, name):
    return f"{PUBLIC_URL}/d/{token}/{quote(name)}"


def curl_command(name, url):
    return f"curl -fC - -o {shlex.quote(name)} {url}"


def deadline(hours):
    return datetime.fromtimestamp(time.time() + hours * 3600).strftime("%Y-%m-%d %H:%M")


def cmd_new(args):
    require_public_url()
    token = create_link(args.hours, args.max, args.note)
    url = f"{PUBLIC_URL}/u/{token}"
    print(f"Link {token_key(token)[:8]}{' for ' + args.note if args.note else ''}: one file, "
          f"up to {human(args.max)}, upload must start by {deadline(args.hours)} "
          f"(once started it stays open until done).\n")
    print("Send this (works on macOS and Linux, resumes if the connection drops):\n")
    print(f"  curl -fsSL {url} | sh -s -- /path/to/file\n")
    print(f"Without resume:  curl -T /path/to/file {url}/")
    print(f"Progress (for them or you): {url}/status")


def cmd_share(args):
    require_public_url()
    try:
        token = create_share(args.path, args.hours, args.note)
    except (OSError, ValueError) as e:
        sys.exit(f"Can't share {args.path}: {e}")
    name = os.path.basename(os.path.realpath(args.path))
    print(f"Link {token_key(token)[:8]}{' for ' + args.note if args.note else ''}: {name} "
          f"({human(os.path.getsize(args.path))}), download must start by {deadline(args.hours)}; "
          f"the link is spent once the whole file has been sent.\n")
    print("Send this (a page with a Download button and a resumable curl command):\n")
    print(f"  {PUBLIC_URL}/d/{token}\n")
    print(f"Straight to the file:  {curl_command(name, download_url(token, name))}")


def progress(entry):
    """Return (bytes moved, seconds since data last moved) for a transfer in flight."""
    if kind_of(entry) == "download":
        active = entry.get("active")
        return entry.get("delivered") or 0, (time.time() - active if active else None)
    try:
        st = os.stat(partial_path(entry))
    except FileNotFoundError:
        return 0, None
    return st.st_size, time.time() - st.st_mtime


def describe(entry):
    """One status line for a link, with progress for transfers in flight."""
    kind = kind_of(entry)
    state = "expired" if is_expired(entry) else entry["state"]
    if state in ("receiving", "sending"):
        got, idle = progress(entry)
        pct = got * 100 / entry["total"] if entry["total"] else 100
        since = datetime.fromtimestamp(time.time() - idle).strftime("%Y-%m-%d %H:%M") if idle is not None else "-"
        activity = "running now" if idle is not None and idle < ACTIVE_WINDOW else f"paused since {since}"
        detail = f"{entry['filename']}  {human(got)} / {human(entry['total'])} ({pct:.0f}%), {activity}"
    elif state == "done":
        finished = datetime.fromtimestamp(entry["finished"]).strftime("%Y-%m-%d %H:%M")
        where = entry["path"] if kind == "upload" else "delivered"
        detail = f"{entry['filename']} ({human(entry['total'])}) at {finished} -> {where}"
    elif state == "open":
        start_by = datetime.fromtimestamp(entry["expires"]).strftime("%Y-%m-%d %H:%M")
        shared = f"{entry['filename']} ({human(entry['total'])}), " if kind == "download" else ""
        detail = f"{shared}waiting, must start by {start_by}"
    else:
        detail = entry["filename"] or "-"
    direction = "in" if kind == "upload" else "out"
    return f"{entry['id']}  {direction:<3}  {state:<9}  {(entry['note'] or '-'):<20}  {detail}"


def duration(seconds):
    hours, rest = divmod(int(seconds), 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours}h{minutes:02d}m" if hours else f"{minutes}m{seconds:02d}s"


def load_entries():
    with tokens() as db:
        return sorted(db.values(), key=lambda e: e["created"])


def cmd_list(_args):
    entries = load_entries()
    if not entries:
        print("No links.")
    for entry in entries:
        print(describe(entry))


def cmd_watch(args):
    """Refresh the transfers in flight with speed and time left until Ctrl-C."""
    previous = {}
    try:
        while True:
            now = time.time()
            lines = []
            for entry in load_entries():
                if entry["state"] not in ("receiving", "sending"):
                    continue
                got, _ = progress(entry)
                line = describe(entry)
                if entry["id"] in previous:
                    then, before = previous[entry["id"]]
                    rate = max(got - before, 0) / (now - then)
                    if rate > 0:
                        left = (entry["total"] - got) / rate
                        line += f", {human(rate)}/s, {duration(left)} left"
                previous[entry["id"]] = (now, got)
                lines.append(line)
            stamp = datetime.now().strftime("%H:%M:%S")
            print(f"\n[{stamp}] " + ("\n".join(lines) if lines else "no transfers in progress"), flush=True)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass


def revoke(link_id):
    """Revoke the one link whose id starts with link_id; return it, or raise ValueError."""
    with tokens() as db:
        matches = [e for e in db.values() if e["id"].startswith(link_id)]
        if len(matches) != 1:
            raise ValueError(f"{len(matches)} links match {link_id!r}")
        entry = matches[0]
        if entry["state"] == "done":
            where = f"the file is at {entry['path']}" if kind_of(entry) == "upload" else "the file was delivered"
            raise ValueError(f"{entry['id']} is already used; {where}")
        entry.update(state="revoked", ended=time.time())
        if kind_of(entry) == "upload":
            with contextlib.suppress(FileNotFoundError):
                os.remove(partial_path(entry))
    return entry


def cmd_revoke(args):
    try:
        entry = revoke(args.id)
    except ValueError as e:
        sys.exit(str(e))
    print(f"Revoked {entry['id']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="run the HTTP service").set_defaults(func=cmd_serve)
    new = sub.add_parser("new", help="create a one-time upload link")
    new.add_argument("--hours", type=float, default=72, help="time the sender has to start (default 72)")
    new.add_argument("--max", type=parse_size, default=parse_size("500G"), help="size limit (default 500G)")
    new.add_argument("--note", default="", help="who or what it is for")
    new.set_defaults(func=cmd_new)
    share = sub.add_parser("share", help="create a one-time download link for a file")
    share.add_argument("path", help="file on this server to send")
    share.add_argument("--hours", type=float, default=72, help="time the recipient has to start (default 72)")
    share.add_argument("--note", default="", help="who or what it is for")
    share.set_defaults(func=cmd_share)
    sub.add_parser("list", help="show links and transfer progress").set_defaults(func=cmd_list)
    watch = sub.add_parser("watch", help="follow transfers in flight until Ctrl-C")
    watch.add_argument("--interval", type=float, default=5, help="seconds between updates (default 5)")
    watch.set_defaults(func=cmd_watch)
    revoke_cmd = sub.add_parser("revoke", help="revoke a link by id")
    revoke_cmd.add_argument("id")
    revoke_cmd.set_defaults(func=cmd_revoke)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
