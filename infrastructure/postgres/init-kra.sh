#!/bin/sh
set -eu

: "${KRA_DB_MIGRATOR_PASSWORD:?KRA_DB_MIGRATOR_PASSWORD must be set}"
: "${KRA_DB_APP_PASSWORD:?KRA_DB_APP_PASSWORD must be set}"
: "${KRA_CDC_PASSWORD:?KRA_CDC_PASSWORD must be set}"
: "${KRA_CDC_STATE_PASSWORD:?KRA_CDC_STATE_PASSWORD must be set}"
: "${KRA_TEMPORAL_DB_PASSWORD:?KRA_TEMPORAL_DB_PASSWORD must be set}"

psql --username "$POSTGRES_USER" --dbname postgres \
    --set="kra_migrator_password=$KRA_DB_MIGRATOR_PASSWORD" \
    --set="kra_app_password=$KRA_DB_APP_PASSWORD" \
    --set="kra_cdc_password=$KRA_CDC_PASSWORD" \
    --set="kra_cdc_state_password=$KRA_CDC_STATE_PASSWORD" \
    --set="kra_temporal_password=$KRA_TEMPORAL_DB_PASSWORD" \
    --set=ON_ERROR_STOP=1 \
    --file=/opt/kra/bootstrap/init.sql
