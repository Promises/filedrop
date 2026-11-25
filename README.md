# filedrop

One-time links for moving **one big file** between your server and someone else, in either direction:

- **Receive:** `filedrop new` gives you a command to send someone. They paste it into a terminal on macOS or
  Linux, and their file lands in its own folder on your server.
- **Send:** `filedrop share <file>` gives you a link. They open it in a browser or use the curl command on the
  page, and the link stops working once the file has been downloaded.

The other side needs nothing beyond a browser or the `curl` they already have. Both directions resume after a
dropped connection.

- **One file, no dependencies:** a single Python 3.10+ script using only the standard library.
- **Resumable:** uploads continue with `curl -C <offset> -T`; downloads support HTTP Range requests, so browsers
  and `curl -C -` pick up where they stopped.
- **Streams:** no size cap and nothing buffered in memory. Partial uploads sit in `<dir>/.partial/` until the last
  byte arrives; shared files are sent straight from where they are.
- **One-time links:** 192-bit random tokens, stored only as SHA-256 hashes. An upload link receives one file, under
  one name, up to a size limit you set. A download link is spent once the whole file has been sent.
- **Link deadlines only limit the start:** once a transfer has started, the link stays open until it is complete
  or you revoke it, however long that takes.

## Receiving a file

```console
$ filedrop new --note "Ola" --max 200G
Link 3f9c2a1b for Ola: one file, up to 200.0 GB, upload must start by 2026-11-28 14:00 (once started it stays open until done).

Send this (works on macOS and Linux, resumes if the connection drops):

  curl -fsSL https://drop.example.com/u/kX2… | sh -s -- /path/to/file

Without resume:  curl -T /path/to/file https://drop.example.com/u/kX2…/
Progress (for them or you): https://drop.example.com/u/kX2…/status
```

The sender sees curl's progress meter, can open `<link>/status` in a browser at any time, and can rerun the same
command to continue after an interruption. The file lands in `<FILEDROP_DIR>/<date>_<note>_<id>/<file name>`.

## Sending a file

```console
$ filedrop share /srv/exports/photos-2026.zip --note "Kari"
Link 7d01e9c4 for Kari: photos-2026.zip (14.2 GB), download must start by 2026-11-28 14:00; the link is spent once the whole file has been sent.

Send this (a page with a Download button and a resumable curl command):

  https://drop.example.com/d/Qm8…

Straight to the file:  curl -fC - -o photos-2026.zip https://drop.example.com/d/Qm8…/photos-2026.zip
```

The link opens a small page with the file name, its size, a Download button and the curl command. Only the
button's URL serves the file. That matters because chat apps fetch links you paste to build a preview: if the
link served the file directly, a preview bot could download a small file and use up the link before the
recipient clicks it.

- **When is the link spent?** When the last byte has gone out. For 10 minutes after that it still answers
  requests that *resume* a download (the last bytes may have been sitting in a buffer when the recipient's
  connection died), but not new full downloads.
- **The file is served in place, not copied.** If it changes or disappears before the download is done, the
  link refuses with "this file has changed" instead of sending something different.
- The service must be able to read the file. With the example systemd unit that means readable by the
  `filedrop` user and not in `/tmp`.

## Commands

| Command | |
|---|---|
| `filedrop new [--hours 72] [--max 500G] [--note TEXT]` | create an upload link and print the command to send |
| `filedrop share PATH [--hours 72] [--note TEXT]` | create a download link for a file on the server |
| `filedrop list` | all links, with progress of transfers in flight |
| `filedrop watch [--interval 5]` | follow transfers live: progress, speed, time left |
| `filedrop revoke <id>` | kill a link (and delete an upload's partial data) |
| `filedrop serve` | run the HTTP service (from systemd) |

```console
$ filedrop watch
[14:02:31] 3f9c2a1b  in   receiving  Ola                   backup.tar  41.2 GB / 180.0 GB (23%), running now, 11.3 MB/s, 3h29m left
7d01e9c4  out  sending    Kari                  photos-2026.zip  9.8 GB / 14.2 GB (69%), running now, 4.1 MB/s, 18m20s left
```

## Install

You need a Linux server with Python 3.10+, and a reverse proxy that terminates TLS. The example uses nginx.

```sh
sudo useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin filedrop
sudo install -d -o filedrop -g filedrop /srv/filedrop
sudo install -D -m 755 filedrop.py /opt/filedrop/filedrop.py
sudo install -m 755 contrib/filedrop /usr/local/bin/filedrop
sudo install -m 644 contrib/filedrop.env.example /etc/filedrop.env     # then set FILEDROP_PUBLIC_URL
sudo install -m 644 contrib/filedrop.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now filedrop

sudo install -m 644 contrib/nginx.conf /etc/nginx/sites-available/filedrop.conf   # set server_name
sudo ln -s ../sites-available/filedrop.conf /etc/nginx/sites-enabled/
sudo certbot --nginx -d drop.example.com

sudo -u filedrop filedrop new --note test
```

The token database belongs to the service user, so run the `filedrop` command as that user.

### Configuration

Set these in `/etc/filedrop.env`:

| Variable | Default | |
|---|---|---|
| `FILEDROP_PUBLIC_URL` | required | the address people use, e.g. `https://drop.example.com` |
| `FILEDROP_DIR` | `/srv/filedrop` | where received files land, one folder per link |
| `FILEDROP_MOUNT` | (no check) | refuse uploads unless this path is a mount point |
| `FILEDROP_MIN_FREE` | `10G` | refuse an upload that would leave less free space than this |
| `FILEDROP_KEEP_DAYS` | `30` | forget finished, revoked and expired links after this many days (`0` = never); files are kept |
| `FILEDROP_STATE` | `/var/lib/filedrop/tokens.json` | token database |
| `FILEDROP_LISTEN` | `127.0.0.1:8099` | address the service listens on |

If received files go to a NAS or USB disk, set `FILEDROP_MOUNT` to its mount point (and `RequiresMountsFor=` in the
unit). Otherwise, when that mount is missing, uploads would quietly fill the server's own disk.

### Proxy notes

- **The proxy must pass `/u/` and `/d/` through without buffering or capping bodies.** With nginx that means
  `client_max_body_size 0`, `proxy_request_buffering off` and `proxy_buffering off`; `contrib/nginx.conf` has the
  full set.
- **Cloudflare's proxy (orange cloud) caps uploads at 100 MB on the free plan.** Make the DNS record DNS-only, or
  run filedrop on a hostname that isn't proxied.
- Turn the proxy's access log off for this site: the URLs contain the secret token.

## Security model

- **The link is the credential.** Anyone holding an upload link can upload one file (up to its size limit) until the
  file is complete; anyone holding a download link can download the file until it has been sent once. Treat links
  like passwords: send them over a channel you trust. `filedrop revoke` cancels one.
- **`curl | sh`:** the sender runs a script from your server. It's short and readable: open the upload link in a
  browser to see exactly what it does. It only reads the one file passed to it and sends it with curl. Senders who
  don't want that can use the plain `curl -T` command, which has no automatic resume.
- **Tokens:** 192 bits from `secrets`, stored as SHA-256 only. The service logs the first 8 characters of the
  hash, never the token. Guessing a token is not feasible, so there is no rate limiting.
- **Received file names** come from the sender. They are reduced to a plain name (no path, no leading dots, no
  control characters, at most 200 bytes), and an existing file is never overwritten: the new one gets
  `name (2).ext`.
- **Disk:** every upload is checked against its link's size limit and against `FILEDROP_MIN_FREE` before any
  data is written.
- **Shared files** are chosen by whoever runs `filedrop share` on the server. The path is stored in the token
  database, and the download page is static HTML with no scripts and a `default-src 'none'` content security policy.
- filedrop doesn't scan, unpack or open what it receives.

The service only listens on localhost, and the systemd unit runs it as an unprivileged user with
`ProtectSystem=strict`: the only paths it can write are its state directory and `FILEDROP_DIR`.

## How resume works

Uploads:

```
sender                                   filedrop
  HEAD /u/<token>/<name>          →      200  Upload-Offset: 0
  PUT  /u/<token>/<name>          →      … connection drops at 41 GB …
  HEAD /u/<token>/<name>          →      200  Upload-Offset: 44023414784
  PUT  (curl -C 44023414784 -T)          Content-Range: bytes 44023414784-193273528319/193273528320
                                  →      201  Received backup.tar (180.0 GB)
```

The uploader retries for as long as the server keeps making progress, and gives up after 10 minutes without any.
An abandoned upload keeps its partial data until you revoke the link; `filedrop list` shows how long it has been
paused.

Downloads are plain HTTP: `curl -C -` and browsers send `Range: bytes=<what they have>-`, and filedrop answers
`206 Partial Content`. An `ETag` built from the file's size and modification time guards browser resumes with
`If-Range`. Running the curl command again after a finished download is harmless: the server answers
`416`, which curl treats as "already complete".

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The tests start a real server on a temporary directory and drive it over HTTP, including with the real uploader
script and curl. They cover resuming in both directions, expiry, revoking, the download grace period, changed
source files, size and free-space limits, and file-name handling.

## License

MIT
