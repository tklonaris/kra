package com.theok.kra.app.payment

import com.stripe.StripeClient
import com.stripe.model.PaymentIntent
import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCreateParams
import com.stripe.service.PaymentIntentService
import com.stripe.service.V1Services
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.shell.payment.generated.jooq.Tables.PAYMENT
import com.theok.kra.shell.payment.workflow.operations.PaymentCreationRequiresReconciliation
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito
import reactor.core.publisher.Flux

/** Covers commercial validation of successful Stripe intent-creation responses. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentIntentCreationValidationIntegrationTest :
    PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `mismatched Stripe creation responses remain uncertain and are not exposed`() = runBlocking {
    val responses: List<(UUID) -> PaymentIntent> =
        listOf(
            { paymentId: UUID -> createdIntent("", paymentId) },
            { paymentId: UUID ->
              createdIntent("pi_test_${UUID.randomUUID()}", paymentId, amount = 101)
            },
            { paymentId: UUID ->
              createdIntent("pi_test_${UUID.randomUUID()}", paymentId, currency = "usd")
            },
            { _: UUID -> createdIntent("pi_test_${UUID.randomUUID()}", UUID.randomUUID()) },
        )
    responses.forEach { response ->
      val payment = registerPaymentForTest()

      assertFailsWith<PaymentCreationRequiresReconciliation> {
        workflowOperations(stripeCreating(response(payment.paymentId)))
            .initialise(payment.paymentRequestId)
      }

      val row =
          fixture.jooq.execute { dsl ->
            Flux.from(
                    dsl.select(
                            PAYMENT.PROVIDER_PAYMENT_ID,
                            PAYMENT.INTENT_CREATION_STATE,
                            PAYMENT.RECONCILIATION_REQUIRED,
                        )
                        .from(PAYMENT)
                        .where(PAYMENT.ID.eq(payment.paymentId)),
                )
                .single()
                .awaitSingle()
          }
      assertNull(row.get(PAYMENT.PROVIDER_PAYMENT_ID))
      assertEquals("UNCERTAIN", row.get(PAYMENT.INTENT_CREATION_STATE))
      assertEquals(true, row.get(PAYMENT.RECONCILIATION_REQUIRED))
    }
  }

  private fun stripeCreating(response: PaymentIntent): StripeClient {
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
        .thenReturn(response)
    return stripe
  }

  private fun createdIntent(
      intentId: String,
      paymentId: UUID,
      amount: Long = 100,
      currency: String = "aud",
  ): PaymentIntent =
      PaymentIntent().apply {
        id = intentId
        clientSecret = "${intentId}_secret"
        this.amount = amount
        this.currency = currency
        metadata = mapOf("kra_payment_id" to paymentId.toString())
        status = "requires_payment_method"
      }
}
