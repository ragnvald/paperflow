#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo ./paperless/fix_paperless_pg_ownership_and_migrate.sh"
  exit 1
fi

echo "Stopping Paperless services..."
systemctl stop paperless-webserver.service paperless-task-queue.service paperless-consumer.service paperless-scheduler.service

echo "Fixing PostgreSQL ownership and privileges for database 'paperless'..."
runuser -u postgres -- psql -d paperless -v ON_ERROR_STOP=1 <<'SQL'
ALTER DATABASE paperless OWNER TO paperless;
ALTER SCHEMA public OWNER TO paperless;

DO $$
DECLARE
  obj record;
BEGIN
  FOR obj IN SELECT tablename FROM pg_tables WHERE schemaname='public' LOOP
    EXECUTE format('ALTER TABLE public.%I OWNER TO paperless', obj.tablename);
  END LOOP;

  FOR obj IN SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema='public' LOOP
    EXECUTE format('ALTER SEQUENCE public.%I OWNER TO paperless', obj.sequence_name);
  END LOOP;

  FOR obj IN SELECT table_name FROM information_schema.views WHERE table_schema='public' LOOP
    EXECUTE format('ALTER VIEW public.%I OWNER TO paperless', obj.table_name);
  END LOOP;
END $$;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO paperless;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO paperless;
SQL

echo "Running Django migrations as paperless user..."
runuser -u paperless -- bash -lc 'cd /opt/paperless/src && /opt/paperless/venv/bin/python manage.py migrate --noinput'

echo "Restarting Paperless services..."
systemctl start paperless-webserver.service paperless-task-queue.service paperless-consumer.service paperless-scheduler.service

echo "Service states:"
systemctl is-active paperless-webserver.service paperless-task-queue.service paperless-consumer.service paperless-scheduler.service

echo "Document count in PostgreSQL:"
runuser -u postgres -- psql -d paperless -Atqc "SELECT COUNT(*) FROM documents_document;"

echo "Done."