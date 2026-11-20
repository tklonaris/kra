package com.theok.kra.shell.payment.configuration.stripe

import java.util.Base64
import kotlin.test.Test
import kotlin.test.assertFailsWith

class PaymentStripePropertiesTest {
  @Test
  fun `rejects missing or malformed webhook secrets`() {
    assertFailsWith<IllegalArgumentException> { properties("") }
    assertFailsWith<IllegalArgumentException> { properties("webhook-secret") }
    assertFailsWith<IllegalArgumentException> { properties("whsec_") }
  }

  @Test
  fun `rejects a nonpositive Stripe blocking I O concurrency`() {
    assertFailsWith<IllegalArgumentException> {
      PaymentStripeProperties(
          secretKey = "sk_test_example",
          webhookSecret = "whsec_example",
          clientSecretEncryptionKey = Base64.getEncoder().encodeToString(ByteArray(32)),
          blockingIoConcurrency = 0,
      )
    }
  }

  private fun properties(webhookSecret: String) =
      PaymentStripeProperties(
          secretKey = "sk_test_example",
          webhookSecret = webhookSecret,
          clientSecretEncryptionKey = Base64.getEncoder().encodeToString(ByteArray(32)),
      )
}
