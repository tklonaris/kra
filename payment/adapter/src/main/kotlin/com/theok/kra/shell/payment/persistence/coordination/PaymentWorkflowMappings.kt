package com.theok.kra.shell.payment.persistence.coordination

import com.theok.kra.core.payment.domain.IntentCreationState
import com.theok.kra.core.payment.domain.PaymentLifecycleState
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.time.OffsetDateTime
import org.jooq.Field
import org.jooq.Record

internal fun Record.toPaymentWorkflowState(databaseNow: Field<OffsetDateTime>) =
    PaymentWorkflowState(
        paymentId = requireNotNull(get(PAYMENT.ID)),
        paymentRequestId = requireNotNull(get(PAYMENT.PAYMENT_REQUEST_ID)),
        amountMinor = requireNotNull(get(PAYMENT_REQUEST.AMOUNT_MINOR)),
        currency = requireNotNull(get(PAYMENT_REQUEST.CURRENCY)).trim(),
        status = PaymentStatus.valueOf(requireNotNull(get(PAYMENT.STATUS))),
        terminalReason = get(PAYMENT.TERMINAL_REASON)?.let(TerminalReason::valueOf),
        intentId = get(PAYMENT.PROVIDER_PAYMENT_ID),
        checkoutDeadlineAt = requireNotNull(get(PAYMENT_REQUEST.CHECKOUT_DEADLINE_AT)).toInstant(),
        processingDeadlineAt =
            requireNotNull(get(PAYMENT_REQUEST.PROCESSING_DEADLINE_AT)).toInstant(),
        cancellationRequested = get(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT) != null,
        cancellationOrigin = get(PAYMENT_REQUEST.CANCELLATION_ORIGIN)?.let(TerminalReason::valueOf),
        intentCreationState =
            IntentCreationState.valueOf(requireNotNull(get(PAYMENT.INTENT_CREATION_STATE))),
        creationOperationId = get(PAYMENT.CREATION_OPERATION_ID),
        stripeIdempotencyKey = get(PAYMENT.PROVIDER_IDEMPOTENCY_KEY),
        creationRequestFingerprint = get(PAYMENT.CREATION_REQUEST_FINGERPRINT),
        creationAttemptedAt = get(PAYMENT.CREATION_ATTEMPTED_AT),
        reconciliationRequired = requireNotNull(get(PAYMENT.RECONCILIATION_REQUIRED)),
        databaseNow = requireNotNull(get(databaseNow)).toInstant(),
    )

internal fun PaymentWorkflowState.toPaymentLifecycleState() =
    PaymentLifecycleState(
        status = status,
        intentCreationState = intentCreationState,
        providerIntentRecorded = intentId != null,
        cancellationRequested = cancellationRequested,
        cancellationOrigin = cancellationOrigin,
    )
