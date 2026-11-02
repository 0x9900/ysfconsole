#! /usr/bin/env python
# vim:fenc=utf-8
#
# Copyright (C) 2026 fred <github-fred@hidzz.com>
#
# Distributed under terms of the BSD 3-Clause license.

import os
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
  bind_address: str = '127.0.0.1'
  bind_port: int = 5000
  log_path: Path = Path("/var/log/mmdvm")
  log_prefix: str = "ysfreflector*.log"
  reflector_name: str = "YSF Reflector"
  reconnect_ms: int = 2000
  max_activity: int = 25
  poll_interval: float = 0.5
  idle_flush: float = 3.0
  max_payload_lines: int = 10000
  log_marker: re.Pattern | None = None

  @classmethod
  def load_config(cls, conf_file: str = None) -> None:
    """Load configuration from TOML file and override attributes."""
    env_config = conf_file if conf_file else os.environ.get("CONFIG_FILE", 'ysfconsole-dev.toml')

    if not env_config:
      raise SystemError('Environment variable CONFIG_FILE not set')

    config_file = Path(env_config)
    if not config_file.exists():
      raise FileNotFoundError(f'No such file or directory: {config_file}')

    with open(config_file, "rb") as fd:
      _config_data = tomllib.load(fd)

    if not _config_data:
      raise SystemError('Configuration error')

    for key, value in _config_data.items():
      if not hasattr(cls, key.lower()):
        current_app.logger.warning('Key %s not a value configuration attribute', key)
        continue

      if key == 'log_marker':
        _value = re.compile(value)
      elif key == 'log_path':
        _value = Path(value)
      else:
        _value = value

      setattr(cls, key, _value)
