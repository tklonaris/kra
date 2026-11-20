--liquibase formatted sql

--changeset tklon:voucher-schema-baseline logicalFilePath:db/changelog/voucher-schema.sql
CREATE TABLE voucher (
    id UUID NOT NULL,
    initial_capacity INTEGER NOT NULL,
    remaining INTEGER NOT NULL,
    CONSTRAINT voucher_pkey PRIMARY KEY (id),
    CONSTRAINT voucher_initial_capacity_positive CHECK (initial_capacity > 0),
    CONSTRAINT voucher_remaining_valid CHECK (remaining >= 0 AND remaining <= initial_capacity)
);

CREATE TABLE voucher_claim (
    request_id UUID NOT NULL,
    voucher_id UUID NOT NULL,
    status VARCHAR(16) NOT NULL,
    rejection_reason VARCHAR(32),
    CONSTRAINT voucher_claim_pkey PRIMARY KEY (request_id),
    CONSTRAINT voucher_claim_status_valid CHECK (status IN ('PENDING', 'SUCCEEDED', 'REJECTED')),
    CONSTRAINT voucher_claim_outcome_valid CHECK (
        (status IN ('PENDING', 'SUCCEEDED') AND rejection_reason IS NULL)
        OR (status = 'REJECTED' AND rejection_reason IN ('CAPACITY_EXHAUSTED', 'VOUCHER_NOT_FOUND'))
    )
);

CREATE TABLE voucher_outbox (
    event_id UUID NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    aggregate_type VARCHAR(32) NOT NULL,
    CONSTRAINT outbox_pkey PRIMARY KEY (event_id),
    CONSTRAINT outbox_event_type_valid CHECK (event_type = 'VoucherRedeemed'),
    CONSTRAINT voucher_outbox_aggregate_type_valid CHECK (aggregate_type = 'voucher')
);
