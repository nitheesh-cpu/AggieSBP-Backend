# PostgreSQL VM Connection Setup

## 🔧 VM Configuration Requirements

### 1. PostgreSQL Configuration in VM

Edit `/etc/postgresql/*/main/postgresql.conf`:
```ini
# Listen on all addresses (or specific VM IP)
listen_addresses = '*'

# Or more secure - specific IP
listen_addresses = 'localhost,VM_IP_ADDRESS'
```

Edit `/etc/postgresql/*/main/pg_hba.conf`:
```ini
# Allow connections from host machine
host    all             all             HOST_IP/32              md5

# Or allow from entire subnet
host    all             all             192.168.1.0/24          md5
```

### 2. Firewall Configuration

**Ubuntu/Debian VM:**
```bash
sudo ufw allow 5432/tcp
sudo ufw reload
```

**CentOS/RHEL VM:**
```bash
sudo firewall-cmd --permanent --add-port=5432/tcp
sudo firewall-cmd --reload
```

### 3. Restart PostgreSQL
```bash
sudo systemctl restart postgresql
```

## 🔍 Connection Testing

### Test from Host Machine:
```bash
# Test connection
psql -h VM_IP_ADDRESS -p 5432 -U username -d database_name

# Or with Python
python -c "import psycopg2; conn = psycopg2.connect('postgresql://user:pass@VM_IP:5432/db'); print('✅ Connected!')"
```

## 📋 Required Information

You'll need:
- ✅ VM IP Address (e.g., `192.168.1.100`)
- ✅ PostgreSQL Port (default: `5432`)
- ✅ Database Name
- ✅ Username & Password
- ✅ Network accessibility between host and VM

## 🛡️ Security Considerations

1. **Use strong passwords**
2. **Limit access by IP** (don't use `0.0.0.0/0`)
3. **Use SSL connections** if possible
4. **Create read-only user** for MCP server:

```sql
-- Create read-only user for MCP
CREATE USER mcp_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE your_database TO mcp_readonly;
GRANT USAGE ON SCHEMA public TO mcp_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_readonly;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO mcp_readonly;

-- For future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_readonly;
``` 