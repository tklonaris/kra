package com.theok.kra.app.payment

import com.stripe.StripeClient
import com.stripe.model.PaymentIntent
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT_REQUEST
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.jooq.impl.DSL
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito
import reactor.core.publisher.Flux

/** Covers a terminal cancellation whose provider cause cannot be corroborated. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentCancellationReconciliationIntegrationTest :
    PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `inconclusive verified cancellation requires operational intervention instead of looping`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val created = PaymentWorkflowStripeFake()
        workflowOperations(created.client).initialise(payment.paymentRequestId)
        val intentId = providerIntentId(payment.paymentId)
        markUndeterminedCancellation(payment)

        val control =
            workflowOperations(cancelledIntent(payment.paymentId, intentId, null))
                .reconcile(payment.paymentRequestId)

        assertTrue(control.complete)
        assertEquals(
            PaymentRequestStatus.FAILED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
        val paymentRow =
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(PAYMENT.TERMINAL_REASON, PAYMENT.RECONCILIATION_REQUIRED)
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(payment.paymentId)),
                  )
                  .single()
                  .awaitSingle()
            }
        assertEquals(
            TerminalReason.CAUSE_UNDETERMINED.name,
            paymentRow.get(PAYMENT.TERMINAL_REASON),
        )
        assertEquals(true, paymentRow.get(PAYMENT.RECONCILIATION_REQUIRED))
      }

  @Test
  fun `verified cancellation cause resolves terminal reconciliation`() = runBlocking {
    val payment = registerPaymentForTest()
    val created = PaymentWorkflowStripeFake()
    workflowOperations(created.client).initialise(payment.paymentRequestId)
    val intentId = providerIntentId(payment.paymentId)
    markUndeterminedCancellation(payment)

    val control =
        workflowOperations(cancelledIntent(payment.paymentId, intentId, "requested_by_customer"))
            .reconcile(payment.paymentRequestId)

    assertTrue(control.complete)
    assertEquals(
        PaymentRequestStatus.COMPLETED.name,
        fixture.database.paymentRequestStatus(payment.paymentRequestId),
    )
    val paymentRow =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT.TERMINAL_REASON, PAYMENT.RECONCILIATION_REQUIRED)
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
        }
    assertEquals(TerminalReason.CUSTOMER_CANCELLED.name, paymentRow.get(PAYMENT.TERMINAL_REASON))
    assertEquals(false, paymentRow.get(PAYMENT.RECONCILIATION_REQUIRED))
  }

  @Test
  fun `concurrent resolution prevents stale failure escalation`() = runBlocking {
    val payment = registerPaymentForTest()
    val created = PaymentWorkflowStripeFake()
    workflowOperations(created.client).initialise(payment.paymentRequestId)
    markUndeterminedCancellation(payment)

    val resolutionApplied = CountDownLatch(1)
    val releaseResolution = CountDownLatch(1)
    val escalationStarted = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(2)
    try {
      val resolved =
          executor.submit<Boolean> {
            runBlocking {
              fixture.workflowTransactions.transaction {
                val resolution =
                    resolveUndeterminedCancellation(
                        payment.paymentId,
                        TerminalReason.CUSTOMER_CANCELLED,
                    )
                resolutionApplied.countDown()
                check(releaseResolution.await(5, TimeUnit.SECONDS)) {
                  "Timed out waiting to commit the cancellation resolution"
                }
                resolution
              }
            }
          }
      assertTrue(resolutionApplied.await(5, TimeUnit.SECONDS))

      val escalated =
          executor.submit<Boolean> {
            runBlocking {
              escalationStarted.countDown()
              fixture.workflowTransactions.transaction {
                markOperationalFailureIfUndeterminedCancellation(
                    payment.paymentId,
                    "Verified Stripe cancellation did not determine its terminal cause",
                )
              }
            }
          }
      assertTrue(escalationStarted.await(5, TimeUnit.SECONDS))
      releaseResolution.countDown()

      assertTrue(resolved.get(5, TimeUnit.SECONDS))
      assertFalse(escalated.get(5, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }
    assertEquals(
        PaymentRequestStatus.COMPLETED.name,
        fixture.database.paymentRequestStatus(payment.paymentRequestId),
    )
  }

  private suspend fun markUndeterminedCancellation(payment: RegisteredPayment) {
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.STATUS, PaymentStatus.CANCELLED.name)
                  .set(PAYMENT.TERMINAL_REASON, TerminalReason.CAUSE_UNDETERMINED.name)
                  .set(PAYMENT.RECONCILIATION_REQUIRED, true)
                  .set(PAYMENT.RECONCILIATION_REASON, "cancellation cause requires reconciliation")
                  .where(PAYMENT.ID.eq(payment.paymentId)),
          )
          .single()
          .awaitSingle()
      Flux.from(
              dsl.update(PAYMENT_REQUEST)
                  .set(PAYMENT_REQUEST.STATUS, PaymentRequestStatus.RECONCILING_PROVIDER_STATE.name)
                  .set(PAYMENT_REQUEST.CANCELLATION_REQUESTED_AT, DSL.currentOffsetDateTime())
                  .set(PAYMENT_REQUEST.CANCELLATION_ORIGIN, TerminalReason.CUSTOMER_CANCELLED.name)
                  .where(PAYMENT_REQUEST.ID.eq(payment.paymentRequestId)),
          )
          .single()
          .awaitSingle()
      Unit
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

  private fun cancelledIntent(
      paymentId: UUID,
      intentId: String,
      cancellationReason: String?,
  ): StripeClient {
    val stripe = Mockito.mock(StripeClient::class.java)
    val services = Mockito.mock(V1Services::class.java)
    val intents = Mockito.mock(PaymentIntentService::class.java)
    Mockito.`when`(stripe.v1()).thenReturn(services)
    Mockito.`when`(services.paymentIntents()).thenReturn(intents)
    Mockito.`when`(intents.retrieve(intentId))
        .thenReturn(
            PaymentIntent().apply {
              id = intentId
              amount = 100
              currency = "aud"
              metadata = mapOf("kra_payment_id" to paymentId.toString())
              status = "canceled"
              this.cancellationReason = cancellationReason
            },
        )
    return stripe
  }
}
