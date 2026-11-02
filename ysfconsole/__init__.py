#! /usr/bin/env python
# vim:fenc=utf-8
#
# Copyright (C) 2026 fred <github-fred@hidzz.com>
#
# Distributed under terms of the BSD 3-Clause license.

import atexit
import glob
import json
import logging
import os
import queue
import re
import threading
import time
from pathlib import Path

from flask import Flask, current_app, render_template
from flask_sock import Sock

from .config import Config

from pprint import pprint


TS_LEN = 27                                   # width of the timestamp prefix
RE_GW = re.compile(
  r'^\s*(?P<call>\w+)\s*:\s*(?P<ip>.+):(?P<port>\d+)\s+(\d+/\d+)\s*$'
)
RE_TX = re.compile(
  r'^Received data from (?P<call>\w+)\s+to\s(?P<dest>\w+)\s+at\s(?P<gw>\w+)'
)
RE_RX = re.compile(
  r'^Received end of transmission'
)
RE_WD = re.compile(
  r'^Network watchdog has expired'
)

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def is_marker(line: str) -> bool:
  return bool(Config.log_marker.search(line))


def _line_body(raw: bytes) -> str:
  return raw.decode("utf-8", errors="replace").rstrip("\r\n")[TS_LEN:]


def find_last_marker(f) -> int:
  """Offset of the last marker line, or EOF if none. `f` must be binary."""
  f.seek(0)
  last = None
  while True:
    pos = f.tell()
    raw = f.readline()
    if not raw:
      return pos if last is None else last
    if is_marker(_line_body(raw)):
      last = pos

# --- Broadcast hub -----------------------------------------------------------
class PayloadHub:
  """
  Watches a log file and emits discrete payloads.

  A payload = the marker line plus every subsequent line, up to (but not
  including) the next marker — or up to an idle timeout.

  Each connected client receives one JSON message per completed payload:

      {"type": "payload", "marker": "...", "lines": ["...", "..."]}
  """

  def __init__(self, path, prefix, poll_interval, idle_flush):
    self.log_dir = Path(path)
    self.pattern = prefix        # e.g. "ysfreflector*.log"
    self.poll_interval = poll_interval
    self.idle_flush = idle_flush

    self.path = path / prefix

    self.clients = set()
    self.lock = threading.Lock()

    # The payload currently being assembled (None if we haven't seen a marker yet).
    self.current_marker = None
    self.current_lines = []
    self.current_tx = None
    self.activity = []
    self.last_line_at = 0.0     # monotonic timestamp of last received line

    # The most recently completed payload, replayed to new clients.
    self.last_payload = None

    self._thread = None
    self._stop = threading.Event()

  # ---- client management ----
  def register(self, client_queue):
    with self.lock:
      self.clients.add(client_queue)
      if self.last_payload is not None:
        client_queue.put(json.dumps(self.last_payload))

  def unregister(self, client_queue):
    with self.lock:
      self.clients.discard(client_queue)

  def _emit(self, payload):
    msg = json.dumps(payload)
    with self.lock:
      self.last_payload = payload
      for q in list(self.clients):
        try:
          q.put_nowait(msg)
        except queue.Full:
          try:
            q.get_nowait()          # discard oldest
            q.put_nowait(msg)
          except (queue.Empty, queue.Full):
            self.clients.discard(q)

  def _flush_connected(self):
    """If we have a buffered payload, emit it and clear the buffer."""
    payload = {
      "type": "connected",
      "marker": self.current_marker,
      "lines": list(self.current_lines),
    }
    pprint(payload)
    self.current_marker = None
    self.current_lines = []
    self._emit(payload)

  def _flush_tx(self):
    payload = {
      "type": "tx",
      "tx": self.current_tx,
    }
    pprint(payload)
    self._emit(payload)

  def start(self):
    log.info('Log watch started')
    if self._thread and self._thread.is_alive():
      return
    self._stop.clear()
    self._thread = threading.Thread(target=self._tail_loop, daemon=True)
    self._thread.start()

  def stop(self):
    self._stop.set()
    if self._thread:
      self._thread.join(timeout=2)

  def _append_line(self, line: str):
    if match := RE_TX.match(line):
      self.current_tx = match.groupdict()
    elif RE_RX.match(line):
      self.current_tx = None
    elif len(self.current_lines) < Config.max_payload_lines:
      if match := RE_GW.match(line):
        self.current_lines.append(match.groupdict())

  def _newest_log(self) -> str | None:
    def mtime(p):
      try:
        return os.stat(p).st_mtime
      except OSError:
        return -1
    files = glob.glob(str(self.log_dir / self.pattern))
    return max(files, key=mtime, default=None)

  def _open(self, name, from_start=False):
    log.info('open %s', name)
    f = open(name, "rb")
    if not from_start:
      f.seek(find_last_marker(f))
    return f


  def _handle_line(self, raw: bytes):
    body = _line_body(raw)
    if not body:
        return

    log.debug("raw=%r body=%r marker=%s cur=%r",
              raw, body, is_marker(body) if body else None, self.current_marker)

    self.last_line_at = time.monotonic()
    if RE_RX.match(body) or RE_WD.match(body):
      self.current_tx = None
      self._flush_tx()
    elif RE_TX.match(body):
      self._append_line(body)
      self._flush_tx()
    elif is_marker(body):
      self.current_marker = body
    elif self.current_marker is not None:
      self._append_line(body)


  def _check_rotation(self, f):
    """
    Called at EOF. Returns `f` if it is still the right file, otherwise a
    replacement file object, or None to force a fresh open.
    """
    try:
      st_path = os.stat(f.name)
    except FileNotFoundError:                  # deleted or renamed away
      self._flush_connected()
      f.close()
      return None

    newest = self._newest_log()
    replaced = st_path.st_ino != os.fstat(f.fileno()).st_ino
    if replaced or (newest and newest != f.name):
      # At EOF, so the old file is fully drained. Switch to the new one.
      self._flush_connected()
      f.close()
      return self._open(newest or f.name, from_start=True)

    if st_path.st_size < f.tell():             # truncated in place
      self._reset_buffer()
      f.seek(find_last_marker(f))
    return f

  def _maybe_idle_flush(self):
    if (self.current_marker is not None
        and time.monotonic() - self.last_line_at >= self.idle_flush):
      self._flush_connected()

  def _reset_buffer(self):
    return
    self.current_marker = None
    self.current_lines = []

  def _tail_loop(self):
    f = None
    pending = b""
    try:
      while not self._stop.is_set():
        try:
          # (Re)open the newest matching file, resuming at its last marker.
          if f is None:
            name = self._newest_log()
            if name is None:
              self._stop.wait(self.poll_interval)
              continue
            f = self._open(name)
            pending = b""

          raw = f.readline()
          if raw:
            pending += raw
            if pending.endswith(b"\n"):        # only handle complete lines
              self._handle_line(pending)
              pending = b""
            continue

          # ---- EOF: rotation / truncation / idle flush ----
          new_f = self._check_rotation(f)
          if new_f is not f:
            f = new_f
            pending = b""
            continue

          self._maybe_idle_flush()
          self._stop.wait(self.poll_interval)

        except Exception:
          log.exception('tail loop error; retrying')
          if f is not None:
            f.close()
            f = None
          pending = b""
          self._reset_buffer()
          self._stop.wait(self.poll_interval)
    finally:
      if f is not None:
        f.close()


def create_app() -> Flask:
  app = Flask(__name__)
  sock = Sock(app)
  sock.init_app(app)

  hub = PayloadHub(Config.log_path, Config.log_prefix, Config.poll_interval, Config.idle_flush)
  app.extensions["payload_hub"] = hub
  hub.start()
  atexit.register(hub.stop)

  # --- Routes ------------------------------------------------------------------
  @app.route("/")
  def index():
    return render_template("index.html", reflector_name=Config.reflector_name)

  @sock.route("/ws/connections")
  def ws_connections(ws):
    q: "queue.Queue[str]" = queue.Queue(maxsize=64)
    hub.register(q)
    try:
      while True:
        try:
          msg = q.get(timeout=5.0)
        except queue.Empty:
          ws.send("")     # keep-alive / detect dead clients
          continue
        ws.send(msg)
    except Exception:
      pass
    finally:
      hub.unregister(q)

  return app


def run():
  Config.load_config()
  create_app().run(host=Config.bind_address, port=Config.bind_port, debug=False)


if __name__ == "__main__":
  run()
