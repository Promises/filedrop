"""End-to-end tests: a real filedrop server on a temporary directory, driven over HTTP.

Run with:  python3 -m unittest discover -s tests -v
"""

import hashlib
import http.client
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import quote

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import filedrop  # noqa: E402

MIB = 1 << 20


def wait_for(condition, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return
        time.sleep(0.02)
    raise AssertionError("timed out waiting for condition")


class FiledropTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.drop = os.path.join(self.tmp, "drop")
        for name, value in {
            "DROP_DIR": self.drop,
            "PARTIAL_DIR": os.path.join(self.drop, ".partial"),
            "STATE": os.path.join(self.tmp, "state", "tokens.json"),
            "MOUNT": "",
            "MIN_FREE": 0,
            "KEEP_DAYS": 30,
            "log": lambda message: None,
        }.items():
            self.patch(name, value)
        self.server = filedrop.make_server("127.0.0.1:0")
        self.port = self.server.server_address[1]
        self.patch("PUBLIC_URL", f"http://127.0.0.1:{self.port}")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.tmp)

    def patch(self, name, value):
        old = getattr(filedrop, name)
        setattr(filedrop, name, value)
        self.addCleanup(setattr, filedrop, name, old)

    # helpers

    def request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        conn.request(method, path, body=body, headers=headers or {})
        resp = conn.getresponse()
        data = resp.read()
        conn.close()
        return resp, data

    def early_reply(self, path, body, headers=None):
        """PUT a body the server may refuse before reading it; return the status code.

        The server answers errors without draining the body, so the send can hit a broken
        pipe (curl copes with that the same way: it still reads the reply).
        """
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        extra = "".join(f"{k}: {v}\r\n" for k, v in (headers or {}).items())
        sock.sendall(f"PUT {path} HTTP/1.1\r\nHost: test\r\nContent-Length: {len(body)}\r\n{extra}\r\n".encode())
        try:
            sock.sendall(body)
        except (BrokenPipeError, ConnectionResetError):
            pass
        reply = sock.recv(4096).decode()
        sock.close()
        return int(reply.split()[1])

    def entry(self, token):
        with filedrop.tokens() as db:
            return dict(db[filedrop.token_key(token)])

    def edit(self, token, **fields):
        with filedrop.tokens() as db:
            db[filedrop.token_key(token)].update(fields)

    def partial_size(self, token):
        try:
            return os.path.getsize(filedrop.partial_path(self.entry(token)))
        except FileNotFoundError:
            return 0

    def upload_running(self, token):
        lock = filedrop._upload_locks.get(filedrop.token_key(token))
        return lock is not None and lock.locked()

    def open_put(self, token, name, data, send):
        """Start a PUT of all of data but send only the first `send` bytes; return the socket."""
        sock = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        head = f"PUT /u/{token}/{name} HTTP/1.1\r\nHost: test\r\nContent-Length: {len(data)}\r\n\r\n"
        sock.sendall(head.encode() + data[:send])
        wait_for(lambda: self.partial_size(token) == send)
        return sock

    def interrupted_put(self, token, name, data, send):
        """Send part of a file, then drop the connection like a lost network would."""
        self.open_put(token, name, data, send).close()
        wait_for(lambda: not self.upload_running(token))

    def resume(self, token, name, data):
        resp, _ = self.request("HEAD", f"/u/{token}/{name}")
        offset = int(resp.getheader("Upload-Offset"))
        rng = f"bytes {offset}-{len(data) - 1}/{len(data)}"
        return self.request("PUT", f"/u/{token}/{name}", body=data[offset:], headers={"Content-Range": rng})

    def received(self, token):
        with open(self.entry(token)["path"], "rb") as f:
            return f.read()

    # tests

    @unittest.skipUnless(shutil.which("curl"), "needs curl")
    def test_uploader_script_sends_file(self):
        token = filedrop.create_link(1, 1 << 30, "script test")
        src = os.path.join(self.tmp, "hello world.bin")
        data = os.urandom(3 * MIB + 17)
        with open(src, "wb") as f:
            f.write(data)
        url = f"{filedrop.PUBLIC_URL}/u/{token}"
        proc = subprocess.run(["sh", "-c", f"curl -fsSL '{url}' | sh -s -- \"$1\"", "sh", src],
                              capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("Received hello world.bin", proc.stdout)
        entry = self.entry(token)
        self.assertEqual(entry["state"], "done")
        self.assertEqual(os.path.basename(entry["path"]), "hello world.bin")
        self.assertIn("script-test", entry["path"])
        self.assertEqual(hashlib.sha256(self.received(token)).digest(), hashlib.sha256(data).digest())
        resp, _ = self.request("HEAD", f"/u/{token}/hello%20world.bin")
        self.assertEqual(resp.getheader("Upload-Complete"), "1")

    def test_resume_after_dropped_connection(self):
        token = filedrop.create_link(1, 1 << 30)
        data = os.urandom(3 * MIB)
        self.interrupted_put(token, "big.bin", data, 2 * MIB)
        self.assertEqual(self.entry(token)["state"], "receiving")
        resp, _ = self.resume(token, "big.bin", data)
        self.assertEqual(resp.status, 201)
        self.assertEqual(self.received(token), data)
        resp, _ = self.request("PUT", f"/u/{token}/big.bin", body=b"again")
        self.assertEqual(resp.status, 410)

    def test_unused_link_expires(self):
        token = filedrop.create_link(1, 1 << 30)
        self.edit(token, expires=time.time() - 1)
        filedrop.housekeeping()
        self.assertEqual(self.entry(token)["state"], "expired")
        resp, _ = self.request("GET", f"/u/{token}")
        self.assertEqual(resp.status, 410)

    def test_started_upload_outlives_expiry(self):
        token = filedrop.create_link(1, 1 << 30)
        data = os.urandom(3 * MIB)
        self.interrupted_put(token, "big.bin", data, 2 * MIB)
        self.edit(token, expires=time.time() - 3600)
        filedrop.housekeeping()
        self.assertEqual(self.entry(token)["state"], "receiving")
        self.assertEqual(self.partial_size(token), 2 * MIB)
        resp, _ = self.request("GET", f"/u/{token}")
        self.assertEqual(resp.status, 200, "uploader script must still be served to resume")
        resp, _ = self.resume(token, "big.bin", data)
        self.assertEqual(resp.status, 201)
        self.assertEqual(self.received(token), data)

    def test_revoke_during_upload(self):
        token = filedrop.create_link(1, 1 << 30)
        data = os.urandom(3 * MIB)
        sock = self.open_put(token, "big.bin", data, 2 * MIB)
        filedrop.revoke(self.entry(token)["id"])
        sock.sendall(data[2 * MIB:])
        reply = sock.recv(4096).decode()
        sock.close()
        self.assertTrue(reply.startswith("HTTP/1.1 410"), reply)
        self.assertEqual(os.listdir(filedrop.PARTIAL_DIR), [])
        resp, _ = self.request("HEAD", f"/u/{token}/big.bin")
        self.assertEqual(resp.status, 410)

    def test_second_upload_while_running_is_retryable(self):
        token = filedrop.create_link(1, 1 << 30)
        data = os.urandom(3 * MIB)
        sock = self.open_put(token, "big.bin", data, 2 * MIB)
        self.assertEqual(self.early_reply(f"/u/{token}/big.bin", data), 423)
        sock.close()

    def test_changed_file_size_is_a_permanent_conflict(self):
        token = filedrop.create_link(1, 1 << 30)
        data = os.urandom(3 * MIB)
        self.interrupted_put(token, "big.bin", data, 2 * MIB)
        rng = f"bytes {2 * MIB}-{4 * MIB - 1}/{4 * MIB}"
        status = self.early_reply(f"/u/{token}/big.bin", os.urandom(2 * MIB), {"Content-Range": rng})
        self.assertEqual(status, 409)

    def test_other_file_name_is_refused(self):
        token = filedrop.create_link(1, 1 << 30)
        self.interrupted_put(token, "a.bin", os.urandom(3 * MIB), 2 * MIB)
        resp, _ = self.request("HEAD", f"/u/{token}/b.bin")
        self.assertEqual(resp.status, 409)

    def test_size_limit(self):
        token = filedrop.create_link(1, 10)
        resp, _ = self.request("PUT", f"/u/{token}/x.bin", body=b"x" * 11)
        self.assertEqual(resp.status, 413)

    def test_path_in_file_name_is_stripped(self):
        token = filedrop.create_link(1, 1 << 30)
        resp, _ = self.request("PUT", f"/u/{token}/..%2F..%2F.passwd", body=b"data")
        self.assertEqual(resp.status, 201)
        path = self.entry(token)["path"]
        self.assertEqual(os.path.basename(path), "passwd")
        self.assertTrue(path.startswith(self.drop + os.sep))

    def test_existing_file_is_not_overwritten(self):
        token = filedrop.create_link(1, 1 << 30, "same")
        directory = filedrop.link_dir(self.entry(token))
        os.makedirs(directory)
        with open(os.path.join(directory, "f.txt"), "w") as f:
            f.write("old")
        self.request("PUT", f"/u/{token}/f.txt", body=b"new")
        self.assertEqual(os.path.basename(self.entry(token)["path"]), "f (2).txt")
        with open(os.path.join(directory, "f.txt")) as f:
            self.assertEqual(f.read(), "old")

    def test_refuses_when_mount_is_missing(self):
        self.patch("MOUNT", os.path.join(self.tmp, "not-a-mount"))
        token = filedrop.create_link(1, 1 << 30)
        resp, _ = self.request("PUT", f"/u/{token}/x.bin", body=b"data")
        self.assertEqual(resp.status, 503)

    def test_refuses_when_disk_would_fill(self):
        self.patch("MIN_FREE", 1 << 60)
        token = filedrop.create_link(1, 1 << 30)
        resp, _ = self.request("PUT", f"/u/{token}/x.bin", body=b"data")
        self.assertEqual(resp.status, 507)

    def test_unknown_token_and_paths(self):
        self.assertEqual(self.request("GET", "/u/" + "x" * 32)[0].status, 404)
        self.assertEqual(self.request("GET", "/")[0].status, 404)
        self.assertEqual(self.request("GET", "/u/short")[0].status, 404)

    def test_housekeeping_forgets_old_links_but_keeps_files(self):
        token = filedrop.create_link(1, 1 << 30)
        self.request("PUT", f"/u/{token}/keep.bin", body=b"data")
        path = self.entry(token)["path"]
        self.edit(token, ended=time.time() - 31 * 86400)
        filedrop.housekeeping()
        with filedrop.tokens() as db:
            self.assertNotIn(filedrop.token_key(token), db)
        self.assertTrue(os.path.exists(path))

    def test_token_is_not_stored(self):
        token = filedrop.create_link(1, 1 << 30)
        with open(filedrop.STATE) as f:
            self.assertNotIn(token, f.read())

    # downloads

    def share(self, data, name="report.pdf", hours=1):
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(data)
        return filedrop.create_share(path, hours), path

    def get(self, token, name="report.pdf", rng=None, method="GET", headers=None):
        headers = dict(headers or {})
        if rng:
            headers["Range"] = rng
        return self.request(method, f"/d/{token}/{quote(name)}", headers=headers)

    def state_becomes(self, token, state):
        """The server records a finished download just after the client has the last byte."""
        wait_for(lambda: self.entry(token)["state"] == state)

    def test_download_works_once(self):
        data = os.urandom(3 * MIB)
        token, _ = self.share(data)
        resp, page = self.request("GET", f"/d/{token}")
        self.assertEqual(resp.status, 200)
        self.assertIn(b"report.pdf", page)
        self.assertIn(b"curl -fC - -o report.pdf", page)
        resp, body = self.get(token)
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, data)
        self.assertEqual(resp.getheader("Accept-Ranges"), "bytes")
        self.state_becomes(token, "done")
        self.assertEqual(self.get(token)[0].status, 410)
        self.assertEqual(self.request("GET", f"/d/{token}")[0].status, 410)

    def test_landing_page_and_head_do_not_use_the_link(self):
        token, _ = self.share(b"x" * 1000)
        self.request("GET", f"/d/{token}")
        self.request("HEAD", f"/d/{token}")
        resp, _ = self.get(token, method="HEAD")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.getheader("Content-Length"), "1000")
        self.assertEqual(self.entry(token)["state"], "open")

    def test_download_resumes_with_range(self):
        data = os.urandom(3 * MIB)
        token, _ = self.share(data)
        resp, first = self.get(token, rng="bytes=0-999")
        self.assertEqual(resp.status, 206)
        self.assertEqual(resp.getheader("Content-Range"), f"bytes 0-999/{len(data)}")
        self.assertEqual(self.entry(token)["state"], "sending")
        resp, rest = self.get(token, rng="bytes=1000-")
        self.assertEqual(resp.status, 206)
        self.assertEqual(first + rest, data)
        self.state_becomes(token, "done")

    def test_grace_period_allows_only_resuming(self):
        data = os.urandom(MIB)
        token, _ = self.share(data)
        self.get(token)
        self.state_becomes(token, "done")
        resp, body = self.get(token, rng="bytes=10-")
        self.assertEqual(resp.status, 206)
        self.assertEqual(body, data[10:])
        self.assertEqual(self.get(token, rng=f"bytes={len(data)}-")[0].status, 416)
        self.assertEqual(self.get(token)[0].status, 410)
        self.assertEqual(self.get(token, rng="bytes=0-")[0].status, 410)
        self.edit(token, finished=time.time() - filedrop.DOWNLOAD_GRACE - 1)
        self.assertEqual(self.get(token, rng="bytes=10-")[0].status, 410)

    @unittest.skipUnless(shutil.which("curl"), "needs curl")
    def test_curl_resumes_and_reruns_cleanly(self):
        data = os.urandom(3 * MIB)
        token, _ = self.share(data)
        out = os.path.join(self.tmp, "out.pdf")
        with open(out, "wb") as f:
            f.write(data[:MIB])  # what an interrupted earlier attempt left behind
        url = filedrop.download_url(token, "report.pdf")
        cmd = ["curl", "-fsS", "-C", "-", "-o", out, url]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        with open(out, "rb") as f:
            self.assertEqual(f.read(), data)
        self.state_becomes(token, "done")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        self.assertEqual(proc.returncode, 0, "rerunning the command after success must not fail: " + proc.stderr)

    def test_started_download_outlives_expiry(self):
        data = os.urandom(MIB)
        token, _ = self.share(data)
        self.get(token, rng="bytes=0-99")
        self.edit(token, expires=time.time() - 3600)
        filedrop.housekeeping()
        self.assertEqual(self.entry(token)["state"], "sending")
        resp, body = self.get(token, rng="bytes=100-")
        self.assertEqual(resp.status, 206)
        self.assertEqual(body, data[100:])

    def test_unused_download_expires(self):
        token, _ = self.share(b"data")
        self.edit(token, expires=time.time() - 1)
        filedrop.housekeeping()
        self.assertEqual(self.entry(token)["state"], "expired")
        self.assertEqual(self.request("GET", f"/d/{token}")[0].status, 410)
        self.assertEqual(self.get(token)[0].status, 410)

    def test_changed_file_is_not_served(self):
        token, path = self.share(b"original")
        with open(path, "ab") as f:
            f.write(b" plus more")
        self.assertEqual(self.get(token)[0].status, 410)
        os.remove(path)
        self.assertEqual(self.get(token)[0].status, 410)

    def test_stale_if_range_sends_whole_file(self):
        data = os.urandom(1000)
        token, _ = self.share(data)
        resp, body = self.get(token, rng="bytes=500-", headers={"If-Range": '"stale"'})
        self.assertEqual(resp.status, 200)
        self.assertEqual(body, data)

    def test_revoked_download(self):
        token, _ = self.share(b"data")
        filedrop.revoke(self.entry(token)["id"])
        self.assertEqual(self.request("GET", f"/d/{token}")[0].status, 410)
        self.assertEqual(self.get(token)[0].status, 410)

    def test_upload_and_download_links_dont_mix(self):
        up = filedrop.create_link(1, 1 << 30)
        down, _ = self.share(b"data")
        self.assertEqual(self.request("GET", f"/d/{up}")[0].status, 404)
        self.assertEqual(self.request("GET", f"/d/{up}/x")[0].status, 404)
        self.assertEqual(self.request("GET", f"/u/{down}")[0].status, 404)
        self.assertEqual(self.request("PUT", f"/u/{down}/x.bin", body=b"data")[0].status, 404)

    def test_unusual_file_names_are_safe(self):
        name = 'rapport "x" \u00e6\u00f8\u00e5 <b>.pdf'
        token, _ = self.share(b"data", name=name)
        resp, _ = self.get(token, name=name, method="HEAD")
        disposition = resp.getheader("Content-Disposition")
        self.assertIn('filename="rapport _x_ ___ <b>.pdf"', disposition)
        self.assertIn("filename*=UTF-8''" + quote(name, safe=""), disposition)
        _, page = self.request("GET", f"/d/{token}")
        self.assertNotIn(b"<b>", page)
        self.assertIn(b"&lt;b&gt;", page)

    def test_share_refuses_what_it_cannot_send(self):
        with self.assertRaises(ValueError):
            filedrop.create_share(self.tmp, 1)
        with self.assertRaises(ValueError):
            filedrop.create_share(os.path.join(self.tmp, "missing"), 1)

    def test_parse_size(self):
        self.assertEqual(filedrop.parse_size("500G"), 500 << 30)
        self.assertEqual(filedrop.parse_size("1.5t"), int(1.5 * (1 << 40)))
        self.assertEqual(filedrop.parse_size("200MB"), 200 << 20)
        self.assertEqual(filedrop.parse_size("1024"), 1024)


if __name__ == "__main__":
    unittest.main()
