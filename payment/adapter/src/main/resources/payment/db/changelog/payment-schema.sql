--liquibase formatted sql

--changeset tklon:payment-request-saga-baseline logicalFilePath:payment/db/changelog/payment-schema.sql
CREATE TABLE payment_request (
    id UUID PRIMARY KEY,
    owner_id VARCHAR(512) NOT NULL,
    idempotency_key UUID NOT NULL,
    purchase_reference UUID NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL CHECK (currency = 'AUD'),
    status VARCHAR(32) NOT NULL CHECK (status IN (
        'ACCEPTED', 'CANCELLED', 'CREATING_PROVIDER_INTENT', 'WAITING_FOR_CUSTOMER',
        'WAITING_FOR_PROCESSING_OUTCOME', 'RECONCILING_CANCELLATION',
        'RECONCILING_PROVIDER_STATE', 'COMPLETED', 'FAILED'
    )),
    checkout_deadline_at TIMESTAMPTZ NOT NULL,
    processing_deadline_at TIMESTAMPTZ NOT NULL,
    cancellation_requested_at TIMESTAMPTZ,
    cancellation_origin VARCHAR(64) CHECK (cancellation_origin IS NULL OR cancellation_origin IN ('CUSTOMER_CANCELLED', 'CHECKOUT_EXPIRED')),
    cancellation_reason VARCHAR(64),
    operational_failure_reason VARCHAR(512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT payment_request_owner_idempotency_key UNIQUE (owner_id, idempotency_key),
    CONSTRAINT payment_request_owner_purchase_reference_key UNIQUE (owner_id, purchase_reference),
    CONSTRAINT payment_request_deadline_order CHECK (processing_deadline_at >= checkout_deadline_at)
);

CREATE TABLE payment (
    id UUID PRIMARY KEY,
    payment_request_id UUID NOT NULL UNIQUE REFERENCES payment_request(id),
    status VARCHAR(32) NOT NULL CHECK (status IN ('REQUESTED', 'AWAITING_CUSTOMER', 'PROCESSING', 'SUCCEEDED', 'CANCELLED')),
    intent_creation_state VARCHAR(16) NOT NULL DEFAULT 'NOT_STARTED' CHECK (intent_creation_state IN ('NOT_STARTED', 'ATTEMPTING', 'KNOWN', 'UNCERTAIN')),
    creation_operation_id UUID,
    provider_idempotency_key VARCHAR(255),
    creation_request_fingerprint VARCHAR(128),
    creation_attempted_at TIMESTAMPTZ,
    provider_payment_id VARCHAR(255) UNIQUE,
    client_secret_nonce BYTEA,
    client_secret_ciphertext BYTEA,
    client_secret_tag BYTEA,
    terminal_reason VARCHAR(64),
    cancellation_attempted_at TIMESTAMPTZ,
    cancellation_resolved_at TIMESTAMPTZ,
    reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE,
    reconciliation_reason VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT payment_request_payment_pair_key UNIQUE (payment_request_id, id),
    CONSTRAINT payment_creation_identity CHECK (
        (intent_creation_state = 'NOT_STARTED' AND creation_operation_id IS NULL AND provider_idempotency_key IS NULL)
        OR (intent_creation_state <> 'NOT_STARTED' AND creation_operation_id IS NOT NULL AND provider_idempotency_key IS NOT NULL)
    ),
    CONSTRAINT payment_client_secret_envelope_check CHECK (
        (client_secret_nonce IS NULL AND client_secret_ciphertext IS NULL AND client_secret_tag IS NULL)
        OR (client_secret_nonce IS NOT NULL AND client_secret_ciphertext IS NOT NULL AND client_secret_tag IS NOT NULL)
    ),
    CONSTRAINT payment_terminal_reason_valid CHECK (
        (status <> 'CANCELLED' AND terminal_reason IS NULL)
        OR (status = 'CANCELLED' AND terminal_reason IN ('CUSTOMER_CANCELLED', 'CHECKOUT_EXPIRED', 'PROVIDER_CANCELLED', 'CAUSE_UNDETERMINED'))
    ),
    CONSTRAINT payment_succeeded_reconciliation_valid CHECK (
        status <> 'SUCCEEDED'
        OR (NOT reconciliation_required AND reconciliation_reason IS NULL)
    ),
    CONSTRAINT payment_cancelled_reconciliation_valid CHECK (
        status <> 'CANCELLED'
        OR (
            (terminal_reason = 'CAUSE_UNDETERMINED' AND reconciliation_required AND reconciliation_reason IS NOT NULL)
            OR (terminal_reason <> 'CAUSE_UNDETERMINED' AND NOT reconciliation_required AND reconciliation_reason IS NULL)
        )
    ),
    CONSTRAINT payment_known_intent_consistency CHECK ((intent_creation_state = 'KNOWN') = (provider_payment_id IS NOT NULL)),
    CONSTRAINT payment_awaiting_customer_setup CHECK (
        status <> 'AWAITING_CUSTOMER' OR (
            provider_payment_id IS NOT NULL AND client_secret_nonce IS NOT NULL
            AND client_secret_ciphertext IS NOT NULL AND client_secret_tag IS NOT NULL
        )
    ),
    CONSTRAINT payment_provider_status_requires_known_intent CHECK (
        status IN ('REQUESTED', 'CANCELLED') OR (
            status IN ('AWAITING_CUSTOMER', 'PROCESSING', 'SUCCEEDED')
            AND intent_creation_state = 'KNOWN' AND provider_payment_id IS NOT NULL
        )
    ),
    CONSTRAINT payment_creation_recovery_state_valid CHECK (
        (intent_creation_state = 'NOT_STARTED' AND creation_request_fingerprint IS NULL AND creation_attempted_at IS NULL)
        OR (intent_creation_state = 'ATTEMPTING' AND creation_request_fingerprint IS NOT NULL AND creation_attempted_at IS NOT NULL AND NOT reconciliation_required)
        OR (intent_creation_state = 'UNCERTAIN' AND creation_request_fingerprint IS NOT NULL AND creation_attempted_at IS NOT NULL AND reconciliation_required AND reconciliation_reason IS NOT NULL)
        OR intent_creation_state = 'KNOWN'
    )
);

CREATE TABLE payment_provider_event (
    provider_event_id VARCHAR(255) PRIMARY KEY,
    payment_id UUID REFERENCES payment(id),
    provider_payment_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    applied_at TIMESTAMPTZ,
    provider_status VARCHAR(64),
    provider_cancellation_reason VARCHAR(64),
    amount_minor BIGINT,
    currency CHAR(3),
    candidate_payment_id UUID,
    CONSTRAINT payment_provider_event_event_payment_key UNIQUE (provider_event_id, payment_id),
    CONSTRAINT payment_provider_event_amount_currency_pair CHECK ((amount_minor IS NULL AND currency IS NULL) OR (amount_minor IS NOT NULL AND currency IS NOT NULL))
);
CREATE INDEX payment_provider_event_provider_payment_idx ON payment_provider_event(provider_payment_id);

CREATE TABLE payment_lifecycle_event (
    event_id UUID PRIMARY KEY,
    payment_id UUID NOT NULL REFERENCES payment(id),
    event_type VARCHAR(64) NOT NULL,
    from_status VARCHAR(32),
    to_status VARCHAR(32),
    terminal_reason VARCHAR(64),
    reconciliation_required BOOLEAN,
    reason VARCHAR(255),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payment_request_lifecycle_event (
    id UUID PRIMARY KEY,
    payment_request_id UUID NOT NULL REFERENCES payment_request(id),
    event_type VARCHAR(64) NOT NULL,
    from_status VARCHAR(32),
    to_status VARCHAR(32),
    reason VARCHAR(512),
    initiated_by_subject VARCHAR(512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
GRANT SELECT, INSERT ON TABLE payment_lifecycle_event, payment_request_lifecycle_event TO kra_app;
REVOKE UPDATE, DELETE ON TABLE payment_lifecycle_event, payment_request_lifecycle_event FROM kra_app;

CREATE TABLE payment_consumer_event (
    event_id UUID PRIMARY KEY,
    payment_request_id UUID NOT NULL REFERENCES payment_request(id),
    payment_id UUID,
    event_type VARCHAR(64) NOT NULL,
    provider_event_id VARCHAR(255),
    delivery_status VARCHAR(32) NOT NULL CHECK (delivery_status IN ('PENDING', 'ACCEPTED', 'SKIPPED', 'DLQ_REQUESTED')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_failure VARCHAR(1000),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT payment_consumer_event_request_payment_fk FOREIGN KEY (payment_request_id, payment_id) REFERENCES payment(payment_request_id, id),
    CONSTRAINT payment_consumer_event_provider_event_payment_fk FOREIGN KEY (provider_event_id, payment_id) REFERENCES payment_provider_event(provider_event_id, payment_id),
    CONSTRAINT payment_consumer_event_provider_event_reference_valid CHECK (
        (event_type = 'PaymentWebhookReceived' AND provider_event_id IS NOT NULL AND payment_id IS NOT NULL)
        OR (event_type <> 'PaymentWebhookReceived' AND provider_event_id IS NULL)
    )
);

CREATE TABLE payment_outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(32) NOT NULL CHECK (aggregate_type = 'payment'),
    payment_request_id UUID NOT NULL REFERENCES payment_request(id),
    payment_id UUID,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    provider_event_id VARCHAR(255),
    CONSTRAINT payment_outbox_request_payment_fk FOREIGN KEY (payment_request_id, payment_id) REFERENCES payment(payment_request_id, id),
    CONSTRAINT payment_outbox_provider_event_payment_fk FOREIGN KEY (provider_event_id, payment_id) REFERENCES payment_provider_event(provider_event_id, payment_id),
    CONSTRAINT payment_outbox_provider_event_valid CHECK (
        (event_type = 'PaymentWebhookReceived' AND provider_event_id IS NOT NULL AND payment_id IS NOT NULL)
        OR (event_type <> 'PaymentWebhookReceived' AND provider_event_id IS NULL)
    )
);
CREATE UNIQUE INDEX payment_outbox_one_terminal_event ON payment_outbox(payment_id)
    WHERE event_type IN ('PaymentSucceeded', 'PaymentCancelled');
CREATE INDEX payment_outbox_outstanding_webhook_handoff_idx ON payment_outbox(payment_id, provider_event_id)
    WHERE event_type = 'PaymentWebhookReceived';

CREATE TABLE payment_webhook_handoff (
    payment_id UUID NOT NULL,
    provider_event_id VARCHAR(255) NOT NULL,
    outbox_event_id UUID NOT NULL,
    CONSTRAINT payment_webhook_handoff_pkey PRIMARY KEY (payment_id, provider_event_id),
    CONSTRAINT payment_webhook_handoff_provider_event_fk FOREIGN KEY (provider_event_id, payment_id) REFERENCES payment_provider_event(provider_event_id, payment_id),
    CONSTRAINT payment_webhook_handoff_outbox_event_fk FOREIGN KEY (outbox_event_id) REFERENCES payment_outbox(event_id) DEFERRABLE INITIALLY DEFERRED
);
