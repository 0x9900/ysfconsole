#! /usr/bin/env python
# vim:fenc=utf-8
#
# Copyright (C) 2026 fred <github-fred@hidzz.com>
#
# Distributed under terms of the BSD 3-Clause license.

import argparse

from ysfconsole import create_app

from .config import Config


def parse_bind(bind):
  host, port = bind.rsplit(":", 1)
  return host, int(port)


def main():
  Config.load_config()
  parser = argparse.ArgumentParser()
  parser.add_argument("--bind", help="Bind address (HOST:PORT)")
  args = parser.parse_args()

  host, port = parse_bind(args.bind) if args.bind else Config.bind_address, Config.bind_port

  app = create_app()
  app.run(
    host=host,
    port=port,
    # ssl_context=("cert.pem", "key.pem"),
    debug=app.config.get("DEBUG", True),
    use_reloader=app.config.get("USE_RELOADER", False)
  )


if __name__ == "__main__":
  main()
