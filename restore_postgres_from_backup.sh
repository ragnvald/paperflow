#!/usr/bin/env bash
set -euo pipefail

BACKUP_FILE="/home/ragnvald/code/paperles_backup/db_backup_20260216200002.sql.gz"
PAPERLESS_CONF="/opt/paperless/paperless.conf"
ASSUME_YES=0
SKIP_SERVICE_STOP=0

usage() {
  cat <<'EOF'
Usage: restore_postgres_from_backup.sh [options]

Options:
  --backup-file PATH     Path to SQL or SQL.GZ dump
  --paperless-conf PATH  Path to paperless.conf (default: /opt/paperless/paperless.conf)
  --yes                  Skip confirmation prompt
  --skip-service-stop    Do not stop/start paperless services
  -h, --help             Show this help

Run this script as root (e.g. sudo ./restore_postgres_from_backup.sh ...)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backup-file)
      BACKUP_FILE="$2"
      shift 2
      ;;
    --paperless-conf)
      PAPERLESS_CONF="$2"
      shift 2
      ;;
    --yes)
      ASSUME_YES=1
      shift
      ;;
    --skip-service-stop)
      SKIP_SERVICE_STOP=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ "${EUID}" -ne 0 ]]; then
  echo "This script must run as root. Use: sudo $0 ..." >&2
  exit 1
fi

if [[ ! -f "$PAPERLESS_CONF" ]]; then
  echo "paperless.conf not found at: $PAPERLESS_CONF" >&2
  exit 1
fi

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "Backup file not found: $BACKUP_FILE" >&2
  exit 1
fi

DB_HOST=""
DB_PORT=""
DB_NAME=""
DB_USER=""
DB_PASS=""

parse_conf_value() {
  local key="$1"
  local value
  value="$(grep -E "^${key}=" "$PAPERLESS_CONF" | tail -n1 | cut -d= -f2- || true)"
  echo "${value%$'\r'}"
}

DB_HOST="$(parse_conf_value PAPERLESS_DBHOST)"
DB_PORT="$(parse_conf_value PAPERLESS_DBPORT)"
DB_NAME="$(parse_conf_value PAPERLESS_DBNAME)"
DB_USER="$(parse_conf_value PAPERLESS_DBUSER)"
DB_PASS="$(parse_conf_value PAPERLESS_DBPASS)"

DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-paperless}"
DB_USER="${DB_USER:-paperless}"

if [[ -z "$DB_PASS" ]]; then
  echo "PAPERLESS_DBPASS is empty in $PAPERLESS_CONF" >&2
  exit 1
fi

echo "Backup:     $BACKUP_FILE"
echo "DB host:    $DB_HOST"
echo "DB port:    $DB_PORT"
echo "DB name:    $DB_NAME"
echo "DB user:    $DB_USER"
echo "Action: drop/recreate target DB and restore backup"

if [[ "$ASSUME_YES" -ne 1 ]]; then
  read -r -p "Continue? This will replace database '$DB_NAME'. [y/N]: " reply
  if [[ ! "$reply" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
  fi
fi

if [[ "$SKIP_SERVICE_STOP" -ne 1 ]]; then
  echo "Stopping Paperless services..."
  systemctl stop paperless-webserver.service paperless-task-queue.service paperless-consumer.service paperless-scheduler.service
fi

echo "Ensuring PostgreSQL service is active..."
systemctl unmask postgresql.service || true
systemctl start postgresql@16-main.service || true
systemctl start postgresql.service

echo "Ensuring DB role and database exist/reset..."
sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
DO
\$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L', '${DB_USER}', '${DB_PASS}');
  ELSE
    EXECUTE format('ALTER ROLE %I WITH LOGIN PASSWORD %L', '${DB_USER}', '${DB_PASS}');
  END IF;
END
\$\$;
SQL

sudo -u postgres psql -v ON_ERROR_STOP=1 <<SQL
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '${DB_NAME}' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS "${DB_NAME}";
CREATE DATABASE "${DB_NAME}" OWNER "${DB_USER}";
SQL

echo "Restoring dump into ${DB_NAME}..."
export PGPASSWORD="$DB_PASS"
if [[ "$BACKUP_FILE" == *.gz ]]; then
  gzip -dc "$BACKUP_FILE" | psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1
else
  psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 < "$BACKUP_FILE"
fi
unset PGPASSWORD

echo "Checking restored document count..."
export PGPASSWORD="$DB_PASS"
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -Atqc "SELECT COUNT(*) FROM documents_document;"
unset PGPASSWORD

if [[ "$SKIP_SERVICE_STOP" -ne 1 ]]; then
  echo "Starting Paperless services..."
  systemctl start paperless-webserver.service paperless-task-queue.service paperless-consumer.service paperless-scheduler.service
fi

echo "PostgreSQL restore complete."