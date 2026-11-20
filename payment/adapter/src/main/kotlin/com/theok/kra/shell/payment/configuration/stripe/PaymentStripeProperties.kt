package com.theok.kra.shell.payment.configuration.stripe

import java.util.Base64
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.validation.annotation.Validated

@Validated
@ConfigurationProperties("payments.stripe")
data class PaymentStripeProperties(
    val secretKey: String,
    val webhookSecret: String,
    val clientSecretEncryptionKey: String,
    val blockingIoConcurrency: Int = DEFAULT_BLOCKING_IO_CONCURRENCY,
) {
  init {
    require(secretKey.startsWith("sk_test_")) { "Only Stripe test-mode secret keys are supported" }
    require(webhookSecret.startsWith("whsec_") && webhookSecret.length > "whsec_".length) {
      "Stripe webhook secret must be a nonblank whsec_ value"
    }
    require(blockingIoConcurrency > 0) { "Stripe blocking I/O concurrency must be positive" }
  }

  fun decodedEncryptionKey(): ByteArray =
      Base64.getDecoder().decode(clientSecretEncryptionKey).also {
        require(it.size == 32) {
          "PAYMENTS_CLIENT_SECRET_ENCRYPTION_KEY must decode to exactly 32 bytes"
        }
      }

  private companion object {
    const val DEFAULT_BLOCKING_IO_CONCURRENCY = 8
  }
}
