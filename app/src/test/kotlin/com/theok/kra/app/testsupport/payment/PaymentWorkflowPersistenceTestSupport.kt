package com.theok.kra.app.testsupport.payment

import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.PaymentStatusDecision
import com.theok.kra.core.payment.domain.PaymentStatusUpdate
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.core.payment.domain.providerStatus
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersistence
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingle
import reactor.core.publisher.Flux

/** Shared payment-row setup and status-transition operations for persistence integration tests. */
internal abstract class PaymentWorkflowPersistenceTestSupport :
    PaymentApplicationIntegrationTestSupport() {
  protected suspend fun paymentStatus(paymentId: UUID): String? =
      fixture.jooq.execute { dsl ->
        Flux.from(dsl.select(PAYMENT.STATUS).from(PAYMENT).where(PAYMENT.ID.eq(paymentId)))
            .single()
            .awaitSingle()
            .get(PAYMENT.STATUS)
      }

  protected suspend fun registerPaymentWithKnownIntent(): RegisteredPayment {
    val acceptedRequest = acceptPaymentRequest()
    val paymentId = registerPayment(acceptedRequest.paymentRequestId)
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.PROVIDER_PAYMENT_ID, "pi_test_${UUID.randomUUID()}")
                  .set(PAYMENT.INTENT_CREATION_STATE, "KNOWN")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "known-intent-key")
                  .where(PAYMENT.ID.eq(paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }
    return RegisteredPayment(acceptedRequest.paymentRequestId, paymentId)
  }

  protected suspend fun PaymentWorkflowPersistence.updatePaymentStatus(
      paymentId: UUID,
      currentStatus: PaymentStatus,
      nextStatus: PaymentStatus,
      terminalReason: TerminalReason? = null,
  ): PaymentStatusTransition =
      applyPaymentStatusChange(
          paymentId,
          currentStatus.providerStatusChangeTo(nextStatus),
          terminalReason,
      )

  private fun PaymentStatus.providerStatusChangeTo(nextStatus: PaymentStatus) =
      when (val decision = providerStatus(PaymentStatusUpdate.valueOf(nextStatus.name))) {
        is PaymentStatusDecision.Change -> decision.statusChange
        PaymentStatusDecision.NoChange,
        is PaymentStatusDecision.ConflictingTerminalOutcome,
        -> error("Expected a domain status change from $this to $nextStatus")
      }
}
