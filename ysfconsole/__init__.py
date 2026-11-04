#! /usr/bin/env python
# vim:fenc=utf-8
#
# Copyright (C) 2026 fred <github-fred@hidzz.com>
#
# Distributed under terms of the BSD 3-Clause license.

from __future__ import annotations

import atexit
import glob
import json
import logging
import os
import queue
import re
import threading
import time
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

from flask import Flask, render_template
from flask_sock import Sock

from .config import Config

__version__ = "0.1.9"

KEEPALIVE_INTERVAL = 30.0

RE_GW = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*(?P<call>\w+)'
  r'\s*:\s*'
  r'(?P<ip_address>\d{1,3}(?:\.\d{1,3}){3}):(?P<port>\d{1,5})\s+(\d+/\d+)$'
)

RE_RM = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*Removing\s(?P<call>\w+)\s*'
  r'\((?P<ip_address>\d{1,3}(?:\.\d{1,3}){3}):(?P<port>\d{1,5})\)'
  r'\s*unlinked$'
)

RE_ADD = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*Adding\s(?P<call>\w+)\s*'
  r'\((?P<ip_address>\d{1,3}(?:\.\d{1,3}){3}):(?P<port>\d{1,5})\)'
)

RE_TX = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*Received data from (?P<call>\w+)\s+to\s(?P<dest>\w+)\s+at\s(?P<gw>\w+)'
)
RE_RX = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*Received (?P<end>end) of transmission'
)
RE_WD = re.compile(
  r'^\w:\s+(?P<time>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+)'
  r'\s*Network watchdog has (?P<end>expired)'
)

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def line_body(raw: bytes) -> str:
  return raw.decode("utf-8", errors="replace").rstrip("\r\n")


def find_last_marker(f: BinaryIO, marker: re.Pattern[str]) -> int:
  """Offset of the last marker line, or EOF if none. `f` must be binary.

  Scans backwards in growing windows, so start-up cost depends on how far
  back the last marker is, not on the size of the log.
  """
  size = f.seek(0, os.SEEK_END)
  window = 1 << 16
  while True:
    start = max(0, size - window)
    f.seek(start)
    if start:
      f.readline()              # discard the (probably partial) first line
    last = None
    while True:
      pos = f.tell()
      raw = f.readline()
      if not raw:
        break
      if marker.search(line_body(raw)):
        last = pos
    if last is not None:
      return last
    if start == 0:
      return size
    window *= 4


# ---- log handling ----
def newest_log(log_dir: Path, pattern: str) -> str | None:
  def mtime(p: str) -> float:
    try:
      return os.stat(p).st_mtime
    except OSError:
      return -1
  files = glob.glob(str(log_dir / pattern))
  return max(files, key=mtime, default=None)


def open_and_seek(name: str, marker: re.Pattern[str], from_start: bool = False) -> BinaryIO:
  # pylint: disable=consider-using-with
  log.info('open %s', name)
  f = open(name, "rb")
  try:
    if not from_start:
      f.seek(find_last_marker(f, marker))
  except Exception:
    f.close()
    raise
  return f


# --- Broadcast hub -----------------------------------------------------------
class PayloadHub:
  """
  Watches a log file and emits discrete payloads.

  A payload = the marker line plus every subsequent line, up to (but not
  including) the next marker — or up to an idle timeout.

  Each connected client receives one JSON message per event:

      {"type": "connected", "marker": "...", "lines": [{...}, ...]}
      {"type": "tx", "tx": {...} | null}

  New clients are sent the latest message of each type on registration.
  """

  # pylint: disable=too-many-instance-attributes, too-many-arguments, too-many-positional-arguments
  def __init__(self, log_dir: str | Path,
               pattern: str,
               poll_interval: float,
               idle_flush: float,
               marker: re.Pattern[str],
               max_payload_lines: int) -> None:
    self.log_dir = Path(log_dir)
    self.pattern = pattern        # e.g. "ysfreflector*.log"
    self.poll_interval = poll_interval
    self.idle_flush = idle_flush
    self.marker = marker
    self.max_payload_lines = max_payload_lines
    self._connected: list[dict[str, Any]] = []   # last known full list

    self.clients: set[queue.Queue[str]] = set()
    self.lock = threading.Lock()

    # State below is only touched by the tail thread.
    self.current_marker: str | None = None
    self.current_lines: list[dict[str, Any]] = []
    self.last_line_at = time.monotonic()

    # Latest serialized message per payload type, replayed to new clients.
    self._replay: dict[str, str] = {}

    self._thread: threading.Thread | None = None
    self._stop = threading.Event()

  # ---- client management ----
  def register(self, client_queue: queue.Queue[str]) -> None:
    with self.lock:
      self.clients.add(client_queue)
      for msg in self._replay.values():
        client_queue.put_nowait(msg)

  def unregister(self, client_queue: queue.Queue[str]) -> None:
    with self.lock:
      self.clients.discard(client_queue)

  def _emit(self, payload: dict[str, Any]) -> None:
    log.debug("emit %r", payload)
    msg = json.dumps(payload)
    with self.lock:
      if payload["type"] not in ("rm", "add"):   # events, not state
        self._replay[payload["type"]] = msg
      for q in list(self.clients):
        try:
          q.put_nowait(msg)
        except queue.Full:
          try:
            q.get_nowait()          # discard oldest
            q.put_nowait(msg)
          except (queue.Empty, queue.Full):
            self.clients.discard(q)

  def _flush_connected(self) -> None:
    """If we have a buffered payload, emit it and clear the buffer."""
    if self.current_marker is None:
      return
    self._connected = list(self.current_lines)
    payload = {"type": "connected", "data": self._connected}
    self._reset_buffer()
    self._emit(payload)

  def _set_tx(self, data: dict[str, Any] | None) -> None:
    self._emit({"type": "tx", "data": data})

  def _set_rm(self, data: dict[str, Any]) -> None:
    log.info(data)
    self._emit({"type": "rm", "data": data})

  def _set_add(self, data: dict[str, Any]) -> None:
    log.info(data)
    self._emit({"type": "add", "data": data})

  def _reset_buffer(self) -> None:
    self.current_marker = None
    self.current_lines = []

  @staticmethod
  def _key(d: dict[str, Any]) -> tuple[str, str, str]:
    return (d["call"], d["ip_address"], d["port"])

  @staticmethod
  def _isodate(value: str) -> str:
    tm = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
    tm.replace(tzinfo=UTC)
    return tm.isoformat()

  @staticmethod
  def parse(m: re.Match[str]) -> dict[str, Any]:
    data = m.groupdict()
    data["time"] = PayloadHub._isodate(data["time"])
    return data

  def _refresh_replay(self) -> None:
    msg = json.dumps({"type": "connected", "data": self._connected})
    with self.lock:
      self._replay["connected"] = msg

  def _apply_rm(self, data: dict[str, Any]) -> None:
    k = self._key(data)
    self.current_lines = [e for e in self.current_lines if self._key(e) != k]
    self._connected = [e for e in self._connected if self._key(e) != k]
    self._refresh_replay()

  def _apply_add(self, data: dict[str, Any]) -> None:
    k = self._key(data)
    if self.current_marker is not None and not any(self._key(e) == k for e in self.current_lines):
      self.current_lines.append(data)
    if not any(self._key(e) == k for e in self._connected):
      self._connected.append(data)
    self._refresh_replay()

  def _handle_line(self, raw: bytes) -> None:
    body = line_body(raw)
    if not body:
      return

    self.last_line_at = time.monotonic()
    if m := RE_RM.match(body):
      data = self.parse(m)
      self._apply_rm(data)
      self._set_rm(data)
    elif m := RE_ADD.match(body):
      data = self.parse(m)
      self._apply_add(data)
      self._set_add(data)
    elif RE_RX.match(body) or RE_WD.match(body):
      self._set_tx(None)
    elif m := RE_TX.match(body):
      self._set_tx(self.parse(m))
    elif self.marker.search(body):
      self._flush_connected()           # previous payload ends here
      self.current_marker = body
    elif self.current_marker is not None and len(self.current_lines) < self.max_payload_lines:
      if m := RE_GW.match(body):
        self.current_lines.append(self.parse(m))

  # ---- lifecycle ----
  def start(self) -> None:
    if self._thread and self._thread.is_alive():
      return
    log.info('Log watch started')
    self._stop.clear()
    self._thread = threading.Thread(target=self._tail_loop, daemon=True)
    self._thread.start()

  def stop(self) -> None:
    self._stop.set()
    if self._thread:
      self._thread.join(timeout=2)

  def _check_rotation(self, f: BinaryIO) -> BinaryIO | None:
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

    newest = newest_log(self.log_dir, self.pattern)
    replaced = st_path.st_ino != os.fstat(f.fileno()).st_ino
    if replaced or (newest and newest != f.name):
      # At EOF, so the old file is fully drained. Switch to the new one.
      self._flush_connected()
      f.close()
      return open_and_seek(newest or f.name, self.marker, from_start=True)

    if st_path.st_size < f.tell():             # truncated in place
      self._reset_buffer()
      f.seek(find_last_marker(f, self.marker))
    return f

  def _maybe_idle_flush(self) -> None:
    if (self.current_marker is not None
        and time.monotonic() - self.last_line_at >= self.idle_flush):
      self._flush_connected()

  def _tail_loop(self) -> None:
    f: BinaryIO | None = None
    pending = b""
    try:
      while not self._stop.is_set():
        try:
          # (Re)open the newest matching file, resuming at its last marker.
          if f is None:
            name = newest_log(self.log_dir, self.pattern)
            if name is None:
              self._stop.wait(self.poll_interval)
              continue
            f = open_and_seek(name, self.marker)
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

        except Exception:    # pylint: disable=broad-exception-caught
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


def get_traffic(limit: int = 10) -> list[dict]:
  name = newest_log(Config.log_path, Config.log_prefix)
  if name is None:
    return []
  recent: deque[dict] = deque(maxlen=limit)
  with open(name, "rb") as fd:
    for raw in fd:
      if m := RE_TX.match(line_body(raw)):
        recent.append(PayloadHub.parse(m))
  return list(reversed(recent))


def create_app() -> Flask:
  app = Flask(__name__)
  app.config["SOCK_SERVER_OPTIONS"] = {
    "ping_interval": 25,
  }
  sock = Sock(app)

  hub = PayloadHub(
    Config.log_path,
    Config.log_prefix,
    Config.poll_interval,
    Config.idle_flush,
    Config.log_marker,
    Config.max_payload_lines,
  )
  app.extensions["payload_hub"] = hub
  hub.start()
  atexit.register(hub.stop)

  # --- Routes ------------------------------------------------------------------
  @app.route("/")
  def index():
    last_traffic = get_traffic()
    reconnect_ms = Config.reconnect_ms
    max_activity = Config.max_activity
    reflector_name = Config.reflector_name
    return render_template("index.html",
                           version=__version__,
                           reflector_name=reflector_name,
                           reflector_id=Config.reflector_id,
                           last_traffic=last_traffic,
                           reconnect_ms=reconnect_ms,
                           max_activity=max_activity)

  @sock.route("/ws/connections")
  def ws_connections(ws):
    q: queue.Queue[str] = queue.Queue(maxsize=64)
    hub.register(q)
    try:
      while True:
        try:
          msg = q.get(timeout=KEEPALIVE_INTERVAL)
        except queue.Empty:
          msg = ""  # keep-alive
        ws.send(msg)
    except Exception:  # pylint: disable=broad-exception-caught
      log.debug("websocket closed", exc_info=True)
    finally:
      hub.unregister(q)

  return app


def run() -> None:
  logging.basicConfig()
  Config.load_config()
  create_app().run(host=Config.bind_address, port=Config.bind_port, debug=False)


if __name__ == "__main__":
  run()
