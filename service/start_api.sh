#!/bin/sh
. venv/bin/activate

nginx && python main.py server start --host 127.0.0.1 --port 8081
