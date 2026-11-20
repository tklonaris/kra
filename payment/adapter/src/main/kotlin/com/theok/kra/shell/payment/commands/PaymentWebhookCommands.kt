package com.theok.kra.shell.payment.commands

import com.fasterxml.jackson.databind.ObjectMapper
import com.stripe.exception.SignatureVerificationException
import com.stripe.net.Webhook
import com.theok.kra.platform.database.json.JooqJsonSupport
import com.theok.kra.shell.payment.configuration.stripe.PaymentStripeProperties
import com.theok.kra.shell.payment.persistence.delivery.PaymentStripeEvent
import com.theok.kra.shell.payment.persistence.delivery.PaymentStripeEventPersister
import com.theok.kra.shell.payment.persistence.delivery.WebhookAcceptance
import java.util.UUID
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

/** HTTP ingress persists a verified Stripe delivery and its worker hand-off atomically. */
@Component
@Profile("payments")
class PaymentWebhookCommands(
    private val stripeEventPersister: PaymentStripeEventPersister,
    private val stripe: PaymentStripeProperties,
    objectMapperProvider: ObjectProvider<ObjectMapper>,
) {
  private val objectMapper = objectMapperProvider.getIfAvailable() ?: JooqJsonSupport.objectMapper()

  @Transactional
  suspend fun accept(rawBody: String, signature: String): WebhookAcceptance {
    val event = verifiedPaymentIntentEvent(rawBody, signature) ?: return WebhookAcceptance.Ignored
    return stripeEventPersister.record(event)
  }

  private fun verifiedPaymentIntentEvent(
      rawBody: String,
      signature: String,
  ): PaymentStripeEvent? =
      try {
        Webhook.constructEvent(rawBody, signature, stripe.webhookSecret)
        val json = requireNotNull(objectMapper.readTree(rawBody))
        val liveMode = json.required("livemode")
        if (!liveMode.isBoolean || liveMode.booleanValue()) throw InvalidStripeWebhook()
        val eventType = requiredText(json, "type")
        val intent = json.required("data").required("object")
        if (
            !eventType.startsWith("payment_intent.") ||
                requiredText(intent, "object") != "payment_intent"
        ) {
          null
        } else {
          PaymentStripeEvent(
              stripeEventId = requiredText(json, "id"),
              stripePaymentIntentId = requiredText(intent, "id"),
              eventType = eventType,
              amountMinor = requiredAmount(intent),
              currency = requiredText(intent, "currency"),
              candidatePaymentId =
                  intent.path("metadata").path("kra_payment_id").asText(null)?.let {
                    runCatching { UUID.fromString(it) }.getOrNull()
                  },
          )
        }
      } catch (_: InvalidStripeWebhook) {
        throw InvalidStripeWebhook()
      } catch (_: SignatureVerificationException) {
        throw InvalidStripeWebhook()
      } catch (_: RuntimeException) {
        // A valid Stripe signature does not make an incomplete event actionable.
        throw InvalidStripeWebhook()
      }

  private fun requiredText(node: com.fasterxml.jackson.databind.JsonNode, field: String): String =
      node.required(field).textValue()?.takeIf(String::isNotBlank) ?: throw InvalidStripeWebhook()

  private fun requiredAmount(node: com.fasterxml.jackson.databind.JsonNode): Long {
    val amount = node.required("amount")
    if (!amount.isIntegralNumber) throw InvalidStripeWebhook()
    return amount.longValue()
  }
}

class InvalidStripeWebhook : RuntimeException() {
  private companion object {
    const val serialVersionUID: Long = 1L
  }
}
