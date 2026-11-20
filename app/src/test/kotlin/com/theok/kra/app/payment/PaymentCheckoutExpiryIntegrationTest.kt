package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentDatabaseFixture.PaymentRequestLifecycleTransition
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import reactor.core.publisher.Flux

/** Verifies immutable checkout and processing deadline behaviour through durable payment state. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentCheckoutExpiryIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `checkout expiry before registration cancels the request without creating a payment`() =
      runBlocking {
        val request = acceptPaymentRequest()
        fixture.database.elapseCheckoutDeadline(request.paymentRequestId)

        val paymentId =
            fixture.workflowTransactions.transaction { registerPayment(request.paymentRequestId) }

        assertNull(paymentId)
        assertEquals(0, fixture.database.paymentCount(request.paymentRequestId))
        assertEquals(
            PaymentRequestStatus.CANCELLED.name,
            fixture.database.paymentRequestStatus(request.paymentRequestId),
        )
        assertEquals(
            PaymentRequestLifecycleTransition(
                PaymentRequestStatus.ACCEPTED.name,
                PaymentRequestStatus.CANCELLED.name,
                "CHECKOUT_EXPIRED",
            ),
            fixture.database.paymentRequestLifecycleTransition(
                request.paymentRequestId,
                "CHECKOUT_EXPIRED",
            ),
        )
      }

  @Test
  fun `checkout expiry prevents registration of a request-only operational failure`() =
      runBlocking {
        val request = acceptPaymentRequest()
        workflowOperations(PaymentWorkflowStripeFake().client)
            .markOperationalFailure(request.paymentRequestId, "registration exhausted retries")
        fixture.database.elapseCheckoutDeadline(request.paymentRequestId)

        val paymentId =
            fixture.workflowTransactions.transaction { registerPayment(request.paymentRequestId) }

        assertNull(paymentId)
        assertEquals(0, fixture.database.paymentCount(request.paymentRequestId))
        assertEquals(
            PaymentRequestStatus.CANCELLED.name,
            fixture.database.paymentRequestStatus(request.paymentRequestId),
        )
        assertEquals(
            PaymentRequestLifecycleTransition(
                PaymentRequestStatus.FAILED.name,
                PaymentRequestStatus.CANCELLED.name,
                "CHECKOUT_EXPIRED",
            ),
            fixture.database.paymentRequestLifecycleTransition(
                request.paymentRequestId,
                "CHECKOUT_EXPIRED",
            ),
        )
      }

  @Test
  fun `checkout expiry cancels a registered payment before provider creation`() = runBlocking {
    val payment = registerPaymentForTest()
    fixture.database.elapseCheckoutDeadline(payment.paymentRequestId)

    val state =
        workflowOperations(PaymentWorkflowStripeFake().client).expire(payment.paymentRequestId)

    assertTrue(state.complete)
    assertEquals(PaymentStatus.CANCELLED.name, paymentStatus(payment.paymentId))
    assertEquals(
        PaymentRequestStatus.COMPLETED.name,
        fixture.database.paymentRequestStatus(payment.paymentRequestId),
    )
  }

  @Test
  fun `checkout expiry reconciles an unrecorded provider creation instead of cancelling locally`() =
      runBlocking {
        val payment = registerPaymentForTest()
        fixture.database.claimProviderIntentCreation(payment.paymentId)
        fixture.database.elapseCheckoutDeadline(payment.paymentRequestId)

        val state =
            workflowOperations(PaymentWorkflowStripeFake().client).expire(payment.paymentRequestId)

        assertFalse(state.complete)
        assertTrue(state.reconciliationRequired)
        assertEquals(PaymentStatus.REQUESTED.name, paymentStatus(payment.paymentId))
        assertEquals(
            PaymentRequestStatus.RECONCILING_PROVIDER_STATE.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )

        val uncertainState =
            workflowOperations(PaymentWorkflowStripeFake().client)
                .expire(
                    payment.paymentRequestId,
                )
        assertFalse(uncertainState.complete)
        assertTrue(uncertainState.reconciliationRequired)
      }

  @Test
  fun `checkout expiry cancels a known provider intent through provider reconciliation`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = PaymentWorkflowStripeFake()
        val operations = workflowOperations(stripe.client)
        operations.initialise(payment.paymentRequestId)
        fixture.database.elapseCheckoutDeadline(payment.paymentRequestId)

        val state = operations.expire(payment.paymentRequestId)

        assertTrue(state.complete)
        assertEquals(1, stripe.cancellationCount())
        assertEquals(PaymentStatus.CANCELLED.name, paymentStatus(payment.paymentId))
        assertEquals(
            PaymentRequestStatus.COMPLETED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
      }

  @Test
  fun `processing continues past checkout expiry until the immutable processing deadline`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = PaymentWorkflowStripeFake()
        val operations = workflowOperations(stripe.client)
        operations.initialise(payment.paymentRequestId)
        fixture.workflowTransactions.transaction {
          assertEquals(
              PaymentStatusTransition.Updated,
              updatePaymentStatus(
                  payment.paymentId,
                  PaymentStatus.AWAITING_CUSTOMER,
                  PaymentStatus.PROCESSING,
              ),
          )
        }
        fixture.database.elapseCheckoutDeadline(payment.paymentRequestId)

        val checkoutState = operations.initialise(payment.paymentRequestId)

        assertTrue(checkoutState.awaitingProcessingOutcome)
        assertEquals(0, stripe.cancellationCount())
        assertEquals(PaymentStatus.PROCESSING.name, paymentStatus(payment.paymentId))

        fixture.database.elapseProcessingDeadline(payment.paymentRequestId)
        val deadlineState = operations.expireProcessingDeadline(payment.paymentRequestId)

        assertFalse(deadlineState.complete)
        assertTrue(deadlineState.reconciliationRequired)
        assertEquals(PaymentStatus.PROCESSING.name, paymentStatus(payment.paymentId))
      }

  @Test
  fun `processing webhook after checkout expiry does not request provider cancellation`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = PaymentWorkflowStripeFake()
        val operations = workflowOperations(stripe.client)
        operations.initialise(payment.paymentRequestId)
        val intentId =
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(PAYMENT.PROVIDER_PAYMENT_ID)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(payment.paymentId)),
                  )
                  .single()
                  .awaitSingle()
                  .get(PAYMENT.PROVIDER_PAYMENT_ID)
            }
        stripe.markProcessing(requireNotNull(intentId))
        fixture.database.elapseCheckoutDeadline(payment.paymentRequestId)

        val state = operations.applyWebhook(payment.paymentId, UUID.randomUUID())

        assertTrue(state.awaitingProcessingOutcome)
        assertEquals(0, stripe.cancellationCount())
        assertEquals(PaymentStatus.PROCESSING.name, paymentStatus(payment.paymentId))
        assertEquals(
            PaymentRequestStatus.WAITING_FOR_PROCESSING_OUTCOME.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
      }

  @Test
  fun `normal provider refresh rejects mismatched commercial terms`() = runBlocking {
    val payment = registerPaymentForTest()
    val stripe = PaymentWorkflowStripeFake()
    val operations = workflowOperations(stripe.client)
    operations.initialise(payment.paymentRequestId)
    val intentId =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT.PROVIDER_PAYMENT_ID)
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
              .get(PAYMENT.PROVIDER_PAYMENT_ID)
        }
    stripe.setAmount(requireNotNull(intentId), 101)

    assertFailsWith<IllegalStateException> {
      operations.applyWebhook(payment.paymentId, UUID.randomUUID())
    }
    assertEquals(PaymentStatus.AWAITING_CUSTOMER.name, paymentStatus(payment.paymentId))
  }
}
