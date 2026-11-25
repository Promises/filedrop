# filedrop

One-time upload links for receiving **one big file** from someone else, straight onto your server.

You run `filedrop new` and send them a single command. They paste it into a terminal on macOS or Linux,
with nothing to install beyond the `curl` they already have:

```sh
curl -fsSL https://drop.example.com/u/<token> | sh -s -- /path/to/file
```

If the connection drops, the upload continues where it stopped, on its own or when they run the same command again.
The finished file lands in its own folder on your server, and the link is spent.

- **One file, no dependencies:** a single Python 3.9+ script using only the standard library.
- **Resumable:** the uploader asks the server how much it already has and continues with `curl -C <offset> -T`.
- **Streams to disk:** no size cap and no buffering in memory or temp files. Partial data sits in
  `<dir>/.partial/` until the last byte arrives.
- **One-time links:** 192-bit random tokens, stored only as SHA-256 hashes. A link can only receive one file, under
  one name, up to a size limit you set.
- **Link deadlines only limit the start:** once an upload has started, the link stays open until the file is
  complete or you revoke it, however long that takes.

## How it looks

```console
$ filedrop new --note "Ola" --max 200G
Link 3f9c2a1b for Ola: one file, up to 200.0 GB, upload must start by 2026-11-28 14:00 (once started it stays open until done).

Send this (works on macOS and Linux, resumes if the connection drops):

  curl -fsSL https://drop.example.com/u/kX2… | sh -s -- /path/to/file

Without resume:  curl -T /path/to/file https://drop.example.com/u/kX2…/
Progress (for them or you): https://drop.example.com/u/kX2…/status

$ filedrop watch
[14:02:31] 3f9c2a1b  receiving  Ola                   backup.tar  41.2 GB / 180.0 GB (23%), uploading now, 11.3 MB/s, 3h29m left
```

The sender sees curl's progress meter and can open `<link>/status` in a browser at any time.
Files land in `<FILEDROP_DIR>/<date>_<note>_<id>/<file name>`.

| Command | |
|---|---|
| `filedrop new [--hours 72] [--max 500G] [--note TEXT]` | create a link and print the command to send |
| `filedrop list` | all links, with progress of uploads in flight |
| `filedrop watch [--interval 5]` | follow uploads live: progress, speed, time left |
| `filedrop revoke <id>` | kill a link and delete its partial data |
| `filedrop serve` | run the HTTP service (from systemd) |

## Install

You need a Linux server with Python 3.9+, and a reverse proxy that terminates TLS. The example uses nginx.

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
| `FILEDROP_PUBLIC_URL` | required | the address senders use, e.g. `https://drop.example.com` |
| `FILEDROP_DIR` | `/srv/filedrop` | where finished files land, one folder per link |
| `FILEDROP_MOUNT` | (no check) | refuse uploads unless this path is a mount point |
| `FILEDROP_MIN_FREE` | `10G` | refuse an upload that would leave less free space than this |
| `FILEDROP_KEEP_DAYS` | `30` | forget finished, revoked and expired links after this many days (`0` = never); files are kept |
| `FILEDROP_STATE` | `/var/lib/filedrop/tokens.json` | token database |
| `FILEDROP_LISTEN` | `127.0.0.1:8099` | address the service listens on |

If the files go to a NAS or USB disk, set `FILEDROP_MOUNT` to its mount point (and `RequiresMountsFor=` in the unit).
Otherwise, when that mount is missing, uploads would quietly fill the server's own disk.

### Proxy notes

- **The proxy must not buffer or cap request bodies.** With nginx that means `client_max_body_size 0` and
  `proxy_request_buffering off`; `contrib/nginx.conf` has the full set.
- **Cloudflare's proxy (orange cloud) caps uploads at 100 MB on the free plan.** Make the DNS record DNS-only, or
  run filedrop on a hostname that isn't proxied.
- Turn the proxy's access log off for this site, or at least for `/u/`: the URLs contain the secret token.

## Security model

- **The link is the credential.** Anyone holding it can upload one file (up to its size limit) until the file is
  complete. Treat it like a password: send it over a channel you trust. `filedrop revoke` cancels it.
- **`curl | sh`:** the sender runs a script from your server. It's short and readable: open the link in a browser
  to see exactly what it does. It only reads the one file passed to it and sends it with curl. Senders who don't
  want that can use the plain `curl -T` command, which has no automatic resume.
- **Tokens:** 192 bits from `secrets`, stored as SHA-256 only. The service logs the first 8 characters of the
  hash, never the token. Guessing a token is not feasible, so there is no rate limiting.
- **File names** come from the sender. They are reduced to a plain name (no path, no leading dots, no control
  characters, at most 200 bytes), and an existing file is never overwritten: the new one gets `name (2).ext`.
- **Disk:** every upload is checked against its link's size limit and against `FILEDROP_MIN_FREE` before any
  data is written.
- filedrop doesn't scan, unpack or open what it receives.

The service only listens on localhost, and the systemd unit runs it as an unprivileged user with
`ProtectSystem=strict`: the only paths it can write are its state directory and `FILEDROP_DIR`.

## How resume works

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

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The tests start a real server on a temporary directory and cover the uploader script, resuming, expiry,
revoking during an upload, size and free-space limits, and file-name cleaning.

## License

MIT
