#!/bin/sh

. ./venv/bin/activate

python main.py start ndc &&  python main.py worker process.NDC --burst && python main.py start label &&  python main.py worker process.Labeling --burst
