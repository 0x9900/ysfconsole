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


RE_GW = re.compile(
  r'^\s*(?P<call>\w+)\s*:\s*(?P<ip>.+):(?P<port>\d+)\s+(\d+/\d+)\s*$'
)

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def is_marker(line: str) -> bool:
  return bool(Config.log_marker.search(line))


def find_last_marker(f) -> int:
  """Return the byte offset of the start of the *last* marker line."""
  f.seek(0)
  last = None
  while True:
    pos = f.tell()
    line = f.readline()
    if not line:
      return pos if last is None else last   # EOF if no marker
    if is_marker(line.rstrip("\r\n")[27:]):
      last = pos


def _line_body(line: str) -> str:
  return line.rstrip("\r\n")[27:]          # strip the timestamp prefix


def find_last_marker(f) -> int:
  """Offset of the last marker line, or EOF if none."""
  f.seek(0)
  last = None
  while True:
    pos = f.tell()
    line = f.readline()
    if not line:
      return pos if last is None else last
    if is_marker(_line_body(line)):
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
    """Broadcast a completed payload to all clients."""
    msg = json.dumps(payload)
    with self.lock:
      self.last_payload = payload
      dead = []
      for q in self.clients:
        try:
          q.put_nowait(msg)
        except Exception:
          dead.append(q)
      for q in dead:
        self.clients.discard(q)

  def _flush_current(self):
    """If we have a buffered payload, emit it and clear the buffer."""
    if self.current_marker is None:
      return
    payload = {
      "type": "payload",
      "marker": self.current_marker,
      "lines": list(self.current_lines),
    }
    self.current_marker = None
    self.current_lines = []
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
    if len(self.current_lines) > Config.max_payload_lines:
      return
    # parse connected GW
    if match := RE_GW.match(line):
      self.current_lines.append(match.groupdict())

  def _newest_log(self) -> str | None:
    def mtime(p):
      try:
        return os.stat(p).st_mtime
      except OSError:              # vanished between glob and stat
        return -1
    files = glob.glob(str(self.log_dir / self.pattern))
    return max(files, key=mtime, default=None)

  def _open(self, name, from_start=False):
    log.info('open %s', name)
    f = open(name, "r", encoding="utf-8", errors="replace")
    if not from_start:
      f.seek(find_last_marker(f))
    return f

  def _reset_buffer(self):
    self.current_marker = None
    self.current_lines = []

  def _tail_loop(self):
    f = None
    name = None
    pending = ""
    try:
      while not self._stop.is_set():
        # (Re)open: newest matching file, resuming at its last marker.
        if f is None:
          name = self._newest_log()
          log.info('Logfile: %s', name)
          if name is None:
            self._stop.wait(self.poll_interval)
            continue
          f = self._open(name)
          pending = ""

        chunk = f.readline()
        if chunk:
          pending += chunk
          if not pending.endswith("\n"):
            continue                       # partial line, wait for the rest
          line, pending = pending, ""
          body = _line_body(line)
          if not body:
            continue                       # genuine blank line, not EOF
          if is_marker(body):
            self._flush_current()
            self.current_marker = body
            self.last_line_at = time.monotonic()
          elif self.current_marker is not None:
            self._append_line(body)
            self.last_line_at = time.monotonic()
          continue

        # ---- EOF: rotation / truncation / idle flush ----
        try:
          newest = self._newest_log()
          if newest and newest != name:
            # We're at EOF, so the old file is fully drained. Switch.
            self._flush_current()
            f.close()
            name = newest
            f = self._open(name, from_start=True)
            pending = ""
            continue
          if os.stat(name).st_size < f.tell():     # truncated in place
            f.seek(find_last_marker(f))
            self._reset_buffer()
            pending = ""
            continue
        except FileNotFoundError:
          f.close()
          f = None
          self._reset_buffer()
          continue

        now = time.monotonic()
        if (self.current_marker is not None
            and (now - self.last_line_at) >= self.idle_flush):
          self._flush_current()

        self._stop.wait(self.poll_interval)
    finally:
      if f is not None:
        f.close()


def create_app() -> Flask:
  app = Flask(__name__)
  sock = Sock(app)
  app.config['SOCK_SERVER_OPTIONS'] = {'ping_interval': 25}
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
          msg = q.get(timeout=1.0)
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
