# YSF Reflector Console/Dashboard

A web-based [console / dashboard][1] for monitoring a YSF (Yaesu System Fusion) Reflector.

## Overview

**ysfconsole** provides a real-time view of your YSF Reflector's activity. Its key particularity is that it uses **WebSockets** to push live updates to the browser — no page refreshes or polling required. Station activity and reflector status are streamed to connected clients as they happen.

## Features

- Real-time reflector monitoring via WebSockets
- Live display of connected stations and activity
- Ready-to-use systemd service file (`ysfconsole.service`)

## Getting Started

Clone the repository and install the package:

```bash
git clone https://github.com/0x9900/ysfconsole.git
cd ysfconsole
python -m venv venv
. ./venv/bin/activate
pip install .
```

For a quick test, edit the confugration file `ysfconsole-dev.toml` and run the command

```bash
ysfrun
```

To deploy as a service, edit and verify the files `ysfconsole-prod.toml`,
install and enable `ysfconsole.service`, and finally configure your http server to proxy the requests to the ysfconsole server.

## License

See the repository for license details.

[1]: https://ysfconsole.bsdworld.org/
