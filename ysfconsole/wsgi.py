#! /usr/bin/env python
# vim:fenc=utf-8
#
# Copyright (C) 2026 fred <github-fred@hidzz.com>
#
# Distributed under terms of the BSD 3-Clause license.

from .config import Config
from ysfconsole import create_app

Config.load_config('ysfconsole-prod.toml')
app = create_app()
