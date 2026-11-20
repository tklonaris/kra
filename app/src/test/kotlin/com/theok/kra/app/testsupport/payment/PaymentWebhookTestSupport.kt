package com.theok.kra.app.testsupport.payment

import com.theok.kra.platform.database.json.JooqJsonSupport
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

internal object PaymentWebhookTestSupport {
  const val WEBHOOK_SECRET = "whsec_test_unused"

  fun paymentIntentSucceeded(
      stripeEventId: String,
      paymentId: UUID,
      intentId: String,
      amountMinor: Long = 100,
      currency: String = "aud",
  ): SignedStripeWebhook {
    val body = paymentIntentBody(stripeEventId, paymentId, intentId, amountMinor, currency)
    return SignedStripeWebhook(body, signature(body))
  }

  fun paymentIntentBody(
      stripeEventId: String,
      paymentId: UUID,
      intentId: String,
      amountMinor: Long = 100,
      currency: String = "aud",
  ): String =
      webhookBody(
          stripeEventId,
          "payment_intent.succeeded",
          "payment_intent",
          intentId,
          paymentId,
          amountMinor,
          currency,
      )

  fun webhookBody(
      stripeEventId: String,
      eventType: String,
      objectType: String,
      objectId: String,
      paymentId: UUID,
      amountMinor: Long = 100,
      currency: String = "aud",
  ): String {
    val objectMapper = JooqJsonSupport.objectMapper()
    val event =
        objectMapper
            .createObjectNode()
            .put("id", stripeEventId)
            .put("type", eventType)
            .put("livemode", false)
    event
        .putObject("data")
        .putObject("object")
        .put("object", objectType)
        .put("id", objectId)
        .put("amount", amountMinor)
        .put("currency", currency)
        .putObject("metadata")
        .put("kra_payment_id", paymentId.toString())
    return objectMapper.writeValueAsString(event)
  }

  fun signature(body: String): String {
    val timestamp = System.currentTimeMillis() / 1000
    val signature =
        Mac.getInstance("HmacSHA256").run {
          init(SecretKeySpec(WEBHOOK_SECRET.toByteArray(), "HmacSHA256"))
          doFinal("$timestamp.$body".toByteArray()).joinToString("") { "%02x".format(it) }
        }
    return "t=$timestamp,v1=$signature"
  }
}

internal data class SignedStripeWebhook(val body: String, val signature: String)
