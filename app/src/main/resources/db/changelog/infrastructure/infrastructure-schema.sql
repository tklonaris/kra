--liquibase formatted sql

--changeset tklon:shared-cdc-state logicalFilePath:db/changelog/infrastructure/infrastructure-schema.sql
CREATE TABLE debezium_offset_storage (
    id VARCHAR(36) NOT NULL,
    offset_key VARCHAR(1255),
    offset_val VARCHAR(1255),
    record_insert_ts TIMESTAMP NOT NULL,
    record_insert_seq INTEGER NOT NULL,
    CONSTRAINT debezium_offset_storage_pkey PRIMARY KEY (id)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE debezium_offset_storage TO kra_cdc_state;

-- Runtime application code must not mutate Liquibase metadata or Debezium's connector offsets.
REVOKE ALL PRIVILEGES ON TABLE databasechangelog, databasechangeloglock, debezium_offset_storage FROM kra_app;

--changeset tklon:shared-cdc-publication logicalFilePath:db/changelog/infrastructure/infrastructure-schema.sql runInTransaction:false
CREATE PUBLICATION kra_outbox_publication FOR TABLE voucher_outbox, payment_outbox;
