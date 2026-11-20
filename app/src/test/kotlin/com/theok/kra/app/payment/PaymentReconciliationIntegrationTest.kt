package com.theok.kra.app.payment

import com.stripe.exception.ApiException
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import reactor.core.publisher.Flux

/** Covers ordinary provider-state reconciliation after a known Stripe intent is recorded. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentReconciliationIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `verified unchanged customer-awaiting status resolves ordinary reconciliation`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = PaymentWorkflowStripeFake()
        workflowOperations(stripe.client).initialise(payment.paymentRequestId)
        workflowOperations(stripe.client)
            .requireReconciliation(payment.paymentRequestId, "temporary local failure")

        val control = workflowOperations(stripe.client).reconcile(payment.paymentRequestId)

        assertFalse(control.reconciliationRequired)
        assertEquals(
            PaymentRequestStatus.WAITING_FOR_CUSTOMER.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
        val row =
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(PAYMENT.RECONCILIATION_REQUIRED, PAYMENT.RECONCILIATION_REASON)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(payment.paymentId)),
                  )
                  .single()
                  .awaitSingle()
            }
        assertEquals(false, row.get(PAYMENT.RECONCILIATION_REQUIRED))
        assertNull(row.get(PAYMENT.RECONCILIATION_REASON))
      }

  @Test
  fun `timer reconciliation retries a cancellation after transient Stripe failure`() = runBlocking {
    val payment = registerPaymentForTest()
    val stripe = PaymentWorkflowStripeFake()
    workflowOperations(stripe.client).initialise(payment.paymentRequestId)
    val intentId = providerIntentId(payment.paymentId)
    stripe.failNextCancellation()
    fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId)

    assertFailsWith<ApiException> {
      workflowOperations(stripe.client).applyCancellation(payment.paymentId)
    }

    assertTrue(workflowOperations(stripe.client).reconcile(payment.paymentRequestId).complete)
    assertEquals(1, stripe.cancellationCount())
    assertNotNull(stripe.cancellationRequest(intentId))
    assertEquals(PaymentStatus.CANCELLED.name, paymentStatus(payment.paymentId))
  }

  @Test
  fun `repeated reconciliation marking retains one lifecycle record`() = runBlocking {
    val payment = registerPaymentForTest()
    val stripe = PaymentWorkflowStripeFake()
    workflowOperations(stripe.client).initialise(payment.paymentRequestId)
    workflowOperations(stripe.client)
        .requireReconciliation(payment.paymentRequestId, "first provider uncertainty")
    workflowOperations(stripe.client)
        .requireReconciliation(payment.paymentRequestId, "later retry must not replace the reason")

    assertEquals(
        1,
        fixture.database.lifecycleEventCount(
            payment.paymentId,
            PaymentLifecycleEventType.RECONCILIATION_REQUIRED,
        ),
    )
    assertEquals(
        1,
        fixture.database.paymentRequestLifecycleEventCount(
            payment.paymentRequestId,
            "RECONCILIATION_REQUIRED",
        ),
    )
    val reason =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT.RECONCILIATION_REASON)
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
              .get(PAYMENT.RECONCILIATION_REASON)
        }
    assertEquals("first provider uncertainty", reason)
  }

  @Test
  fun `requires capture and unknown Stripe statuses retain reconciliation`() = runBlocking {
    val stripe = PaymentWorkflowStripeFake()
    listOf("requires_capture", "future_provider_status").forEach { status ->
      val payment = registerPaymentForTest()
      workflowOperations(stripe.client).initialise(payment.paymentRequestId)
      workflowOperations(stripe.client)
          .requireReconciliation(payment.paymentRequestId, "temporary provider uncertainty")
      stripe.setStatus(providerIntentId(payment.paymentId), status)

      assertTrue(
          workflowOperations(stripe.client)
              .reconcile(payment.paymentRequestId)
              .reconciliationRequired,
      )
      assertEquals(
          PaymentRequestStatus.RECONCILING_PROVIDER_STATE.name,
          fixture.database.paymentRequestStatus(payment.paymentRequestId),
      )
    }
  }

  private suspend fun providerIntentId(paymentId: UUID): String =
      fixture.jooq.execute { dsl ->
        Flux.from(
                dsl.select(PAYMENT.PROVIDER_PAYMENT_ID)
                    .from(PAYMENT)
                    .where(PAYMENT.ID.eq(paymentId)),
            )
            .single()
            .awaitSingle()
            .get(PAYMENT.PROVIDER_PAYMENT_ID) ?: error("Payment $paymentId has no provider intent")
      }
}
