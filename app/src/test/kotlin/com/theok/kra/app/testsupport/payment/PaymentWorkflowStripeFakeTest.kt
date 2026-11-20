package com.theok.kra.app.testsupport.payment

import com.stripe.net.RequestOptions
import com.stripe.param.PaymentIntentCreateParams
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class PaymentWorkflowStripeFakeTest {
  @Test
  fun `preserves create idempotency and rejects conflicting parameters`() {
    val stripe = PaymentWorkflowStripeFake()
    val key = "test-key-${UUID.randomUUID()}"
    val matching = paymentIntentParams(100)

    val first =
        stripe.client
            .v1()
            .paymentIntents()
            .create(matching, RequestOptions.builder().setIdempotencyKey(key).build())
    val second =
        stripe.client
            .v1()
            .paymentIntents()
            .create(matching, RequestOptions.builder().setIdempotencyKey(key).build())

    assertEquals(first.id, second.id)
    assertEquals(1, stripe.createCount())
    assertFailsWith<IllegalStateException> {
      stripe.client
          .v1()
          .paymentIntents()
          .create(paymentIntentParams(200), RequestOptions.builder().setIdempotencyKey(key).build())
    }
  }

  private fun paymentIntentParams(amount: Long): PaymentIntentCreateParams =
      PaymentIntentCreateParams.builder()
          .setAmount(amount)
          .setCurrency("aud")
          .putMetadata("kra_payment_id", UUID.randomUUID().toString())
          .build()
}
