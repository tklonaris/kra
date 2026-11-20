package com.theok.kra.app.payment

import com.stripe.StripeClient
import com.stripe.exception.ApiException
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCreateParams
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.workflow.operations.PaymentCreationRequiresReconciliation
import com.theok.kra.shell.payment.workflow.temporal.PaymentWorkflowActivitiesImpl
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito
import reactor.core.publisher.Flux

/** Covers a Stripe creation whose result is unknown after the create request. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentCreationUncertaintyIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `timer reconciliation reinitialises an accepted request without a payment`() = runBlocking {
    val request = acceptPaymentRequest()
    val stripe = PaymentWorkflowStripeFake()

    val control = workflowOperations(stripe.client).reconcile(request.paymentRequestId)

    assertFalse(control.complete)
    assertEquals(1, stripe.createCount())
    assertEquals(
        PaymentRequestStatus.WAITING_FOR_CUSTOMER.name,
        fixture.database.paymentRequestStatus(request.paymentRequestId),
    )
  }

  @Test
  fun `cancellation racing a successful Stripe creation retains the returned intent`() =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = PaymentWorkflowStripeFake {
          runBlocking {
            fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId)
          }
        }

        val state = workflowOperations(stripe.client).initialise(payment.paymentRequestId)

        assertTrue(state.complete)
        assertEquals(1, stripe.createCount())
        assertEquals(1, stripe.cancellationCount())
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
        assertNotNull(intentId)
        assertEquals(PaymentStatus.CANCELLED.name, paymentStatus(payment.paymentId))
        assertEquals(
            PaymentRequestStatus.COMPLETED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
      }

  @Test
  fun `lost Stripe creation response is marked uncertain without a retrying create`(): Unit =
      runBlocking {
        val payment = registerPaymentForTest()
        val stripe = Mockito.mock(StripeClient::class.java)
        val services = Mockito.mock(V1Services::class.java)
        val intents = Mockito.mock(PaymentIntentService::class.java)
        Mockito.`when`(stripe.v1()).thenReturn(services)
        Mockito.`when`(services.paymentIntents()).thenReturn(intents)
        Mockito.`when`(
                intents.create(
                    Mockito.any(PaymentIntentCreateParams::class.java),
                    Mockito.any(RequestOptions::class.java),
                ),
            )
            .thenThrow(ApiException("response lost", null, null, 500, null))

        assertFailsWith<PaymentCreationRequiresReconciliation> {
          workflowOperations(stripe).initialise(payment.paymentRequestId)
        }
        val row =
            fixture.jooq.execute { dsl ->
              Flux.from(
                      dsl.select(
                              PAYMENT.INTENT_CREATION_STATE,
                              PAYMENT.RECONCILIATION_REQUIRED,
                              PAYMENT.RECONCILIATION_REASON,
                          )
                          .from(PAYMENT)
                          .where(PAYMENT.ID.eq(payment.paymentId)),
                  )
                  .single()
                  .awaitSingle()
            }

        assertEquals("UNCERTAIN", row.get(PAYMENT.INTENT_CREATION_STATE))
        assertEquals(true, row.get(PAYMENT.RECONCILIATION_REQUIRED))
        assertEquals(
            "Stripe create failed before its outcome was known",
            row.get(PAYMENT.RECONCILIATION_REASON),
        )
        Mockito.verify(intents, Mockito.times(1))
            .create(
                Mockito.any(PaymentIntentCreateParams::class.java),
                Mockito.any(RequestOptions::class.java),
            )
      }

  @Test
  fun `uncertain creation activity remains open for verified webhook recovery`() = runBlocking {
    val payment = registerPaymentForTest()
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "stale-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(
                      PAYMENT.CREATION_ATTEMPTED_AT,
                      OffsetDateTime.now(ZoneOffset.UTC).minusHours(24),
                  )
                  .where(PAYMENT.ID.eq(payment.paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }
    val stripe = Mockito.mock(StripeClient::class.java)

    val snapshot =
        PaymentWorkflowActivitiesImpl(workflowOperations(stripe))
            .initialise(payment.paymentRequestId)
    val row =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(
                          PAYMENT.INTENT_CREATION_STATE,
                          PAYMENT.RECONCILIATION_REQUIRED,
                          PAYMENT.RECONCILIATION_REASON,
                      )
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
        }

    assertFalse(snapshot.complete)
    assertFalse(snapshot.awaitingProcessingOutcome)
    assertTrue(snapshot.reconciliationRequired)
    assertEquals("UNCERTAIN", row.get(PAYMENT.INTENT_CREATION_STATE))
    assertTrue(requireNotNull(row.get(PAYMENT.RECONCILIATION_REQUIRED)))
    assertEquals(
        "Stripe creation outcome requires reconciliation",
        row.get(PAYMENT.RECONCILIATION_REASON),
    )
    Mockito.verifyNoInteractions(stripe)
  }

  @Test
  fun `workflow reconciliation promotes an in-flight creation to uncertain`() = runBlocking {
    val payment = registerPaymentForTest()
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                  .set(PAYMENT.CREATION_OPERATION_ID, UUID.randomUUID())
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "in-flight-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(PAYMENT.CREATION_ATTEMPTED_AT, OffsetDateTime.now(ZoneOffset.UTC))
                  .where(PAYMENT.ID.eq(payment.paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }

    workflowOperations(Mockito.mock(StripeClient::class.java))
        .requireReconciliation(
            payment.paymentRequestId,
            "local persistence failed after Stripe creation",
        )

    val row =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(
                          PAYMENT.INTENT_CREATION_STATE,
                          PAYMENT.RECONCILIATION_REQUIRED,
                          PAYMENT.RECONCILIATION_REASON,
                      )
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
        }

    assertEquals("UNCERTAIN", row.get(PAYMENT.INTENT_CREATION_STATE))
    assertTrue(requireNotNull(row.get(PAYMENT.RECONCILIATION_REQUIRED)))
    assertEquals(
        "local persistence failed after Stripe creation",
        row.get(PAYMENT.RECONCILIATION_REASON),
    )
  }

  @Test
  fun `retrying a creation claim retains its original idempotency window`() = runBlocking {
    val payment = registerPaymentForTest()
    val operationId = UUID.randomUUID()
    val firstAttemptedAt = OffsetDateTime.now(ZoneOffset.UTC).minusHours(1).withNano(0)
    fixture.jooq.execute { dsl ->
      Flux.from(
              dsl.update(PAYMENT)
                  .set(PAYMENT.INTENT_CREATION_STATE, "ATTEMPTING")
                  .set(PAYMENT.CREATION_OPERATION_ID, operationId)
                  .set(PAYMENT.PROVIDER_IDEMPOTENCY_KEY, "stable-idempotency-key")
                  .set(PAYMENT.CREATION_REQUEST_FINGERPRINT, "100:AUD:automatic-card")
                  .set(PAYMENT.CREATION_ATTEMPTED_AT, firstAttemptedAt)
                  .where(PAYMENT.ID.eq(payment.paymentId)),
          )
          .single()
          .awaitSingle()
      Unit
    }

    fixture.workflowTransactions.transaction {
      assertTrue(
          claimIntentCreation(
              payment.paymentId,
              operationId,
              "stable-idempotency-key",
              "100:AUD:automatic-card",
          ),
      )
    }

    val attemptedAt =
        fixture.jooq.execute { dsl ->
          Flux.from(
                  dsl.select(PAYMENT.CREATION_ATTEMPTED_AT)
                      .from(PAYMENT)
                      .where(PAYMENT.ID.eq(payment.paymentId)),
              )
              .single()
              .awaitSingle()
              .get(PAYMENT.CREATION_ATTEMPTED_AT)
        }

    assertEquals(firstAttemptedAt.toInstant(), requireNotNull(attemptedAt).toInstant())
  }
}
