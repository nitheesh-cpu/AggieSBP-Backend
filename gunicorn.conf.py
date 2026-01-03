# Gunicorn configuration file for AggieRMP API

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = 4  # Adjust based on CPU cores (2 * cores + 1)
worker_class = "uvicorn.workers.UvicornWorker"
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50

# Timeout settings
timeout = 30
keepalive = 2
graceful_timeout = 30

# Logging
accesslog = "-"  # Log to stdout
errorlog = "-"  # Log to stderr
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process naming
proc_name = "aggiermp-api"

# Server mechanics
daemon = False
pidfile = "/tmp/gunicorn.pid"
user = None  # Set to appropriate user in production
group = None  # Set to appropriate group in production
tmp_upload_dir = None

# SSL (uncomment and configure for HTTPS)
# keyfile = "/path/to/keyfile"
# certfile = "/path/to/certfile"

# Application-specific
module = "src.aggiermp.api.main:app"
