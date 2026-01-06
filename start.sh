#!/bin/bash
set -e
echo "Starting AggieRMP service..." >> /tmp/aggiermp.log
cd /home/opc/backend
echo "Changed to directory: $(pwd)" >> /tmp/aggiermp.log
source .venv/bin/activate
echo "Virtual environment activated" >> /tmp/aggiermp.log
echo "Python path: $(which python)" >> /tmp/aggiermp.log
exec python -m gunicorn src.aggiermp.api.main:app -w 4 --bind 0.0.0.0:8000

