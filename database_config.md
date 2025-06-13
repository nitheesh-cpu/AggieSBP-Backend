# Database Performance Optimization Guide

## Current Optimizations Implemented

### 1. Connection Pooling
The API now uses SQLAlchemy connection pooling for persistent database connections:

- **Pool Size**: 10 persistent connections (configurable)
- **Max Overflow**: 20 additional connections when pool is full
- **Pool Timeout**: 30 seconds to wait for available connection
- **Pool Recycle**: 3600 seconds (1 hour) before recycling connections
- **Pre-ping**: Validates connections before use

### 2. Performance Monitoring
- Database query performance logging
- Slow query detection (>1 second)
- Connection pool status monitoring
- Health check endpoints

### 3. Session Management
- Singleton engine pattern (one engine per application)
- Optimized session factory
- Proper session lifecycle management

## Environment Configuration

### Current Remote Database Setup
```env
POSTGRES_HOST=207.211.177.26
POSTGRES_PORT=5432
POSTGRES_USER=rmp_user
POSTGRES_PASSWORD=IS8L3FOR8xgvXUta47OcyHFr
POSTGRES_DATABASE=rmp_app
```

### For Local Database Setup
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_local_password
POSTGRES_DATABASE=aggiermp_local
```

## Setting Up Local PostgreSQL Database

### 1. Install PostgreSQL

#### Windows:
1. Download PostgreSQL from https://www.postgresql.org/download/windows/
2. Run the installer and follow the setup wizard
3. Remember the password you set for the `postgres` user
4. Default port is 5432

#### Alternative - Using Docker:
```bash
# Pull PostgreSQL image
docker pull postgres:15

# Run PostgreSQL container
docker run --name aggiermp-postgres \
  -e POSTGRES_PASSWORD=your_password \
  -e POSTGRES_DB=aggiermp_local \
  -p 5432:5432 \
  -d postgres:15
```

### 2. Create Database and User
```sql
-- Connect to PostgreSQL as superuser
psql -U postgres

-- Create database
CREATE DATABASE aggiermp_local;

-- Create user (optional - you can use postgres user)
CREATE USER aggiermp_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE aggiermp_local TO aggiermp_user;

-- Exit psql
\q
```

### 3. Export Data from Remote Database
```bash
# Export schema and data from remote database
pg_dump -h 207.211.177.26 -U rmp_user -d rmp_app -f aggiermp_backup.sql

# Or export only schema
pg_dump -h 207.211.177.26 -U rmp_user -d rmp_app --schema-only -f aggiermp_schema.sql

# Or export only data
pg_dump -h 207.211.177.26 -U rmp_user -d rmp_app --data-only -f aggiermp_data.sql
```

### 4. Import Data to Local Database
```bash
# Import full backup
psql -U postgres -d aggiermp_local -f aggiermp_backup.sql

# Or import schema then data separately
psql -U postgres -d aggiermp_local -f aggiermp_schema.sql
psql -U postgres -d aggiermp_local -f aggiermp_data.sql
```

### 5. Update Environment Variables
Update your `.env` file:
```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_local_password
POSTGRES_DATABASE=aggiermp_local
```

### 6. Verify Local Setup
```bash
# Test connection
psql -U postgres -d aggiermp_local -c "SELECT COUNT(*) FROM departments;"

# Check table sizes
psql -U postgres -d aggiermp_local -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
"
```

## Performance Optimizations for Local Database

### 1. PostgreSQL Configuration
Edit `postgresql.conf` (usually in `/var/lib/postgresql/data/` or `C:\Program Files\PostgreSQL\15\data\`):

```conf
# Memory settings
shared_buffers = 256MB          # 25% of RAM for dedicated server
effective_cache_size = 1GB      # 75% of RAM
work_mem = 4MB                  # Per-operation memory
maintenance_work_mem = 64MB     # For maintenance operations

# Connection settings
max_connections = 100           # Adjust based on your needs

# Query planner settings
random_page_cost = 1.1          # For SSD storage
effective_io_concurrency = 200  # For SSD storage

# Write-ahead logging
wal_buffers = 16MB
checkpoint_completion_target = 0.9
```

### 2. Create Indexes for Better Performance
```sql
-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_gpa_data_dept_course ON gpa_data(dept, course_number);
CREATE INDEX IF NOT EXISTS idx_gpa_data_year_semester ON gpa_data(year, semester);
CREATE INDEX IF NOT EXISTS idx_reviews_course_code ON reviews(course_code);
CREATE INDEX IF NOT EXISTS idx_reviews_professor_id ON reviews(professor_id);
CREATE INDEX IF NOT EXISTS idx_departments_short_name ON departments(short_name);

-- Composite indexes for complex queries
CREATE INDEX IF NOT EXISTS idx_gpa_data_composite ON gpa_data(dept, course_number, year, semester);
CREATE INDEX IF NOT EXISTS idx_reviews_composite ON reviews(course_code, professor_id);
```

## API Endpoints for Monitoring

### Health Check
```bash
curl http://localhost:8000/health
```

### Database Status
```bash
curl http://localhost:8000/db-status
```

## Expected Performance Improvements

### Remote Database (Current):
- **Before**: ~500-1000ms per request (connection overhead)
- **After**: ~100-300ms per request (connection pooling)

### Local Database (Future):
- **Expected**: ~10-50ms per request (no network latency)
- **Additional benefits**: No network dependency, better reliability

## Troubleshooting

### Common Issues:

1. **Connection Pool Exhausted**
   - Increase `pool_size` or `max_overflow`
   - Check for connection leaks in application code

2. **Slow Queries**
   - Check database indexes
   - Analyze query execution plans
   - Consider query optimization

3. **Memory Issues**
   - Adjust PostgreSQL memory settings
   - Monitor connection pool usage

4. **Local Database Connection Issues**
   - Verify PostgreSQL service is running
   - Check firewall settings
   - Verify connection credentials

### Monitoring Commands:
```bash
# Check active connections
psql -U postgres -d aggiermp_local -c "SELECT count(*) FROM pg_stat_activity;"

# Check database size
psql -U postgres -d aggiermp_local -c "SELECT pg_size_pretty(pg_database_size('aggiermp_local'));"

# Check slow queries (if logging enabled)
tail -f /var/log/postgresql/postgresql-15-main.log
```

## Next Steps

1. **Immediate**: The connection pooling optimizations are now active
2. **Short-term**: Set up local PostgreSQL database using this guide
3. **Long-term**: Consider additional optimizations like query caching, read replicas, or database partitioning for very large datasets 