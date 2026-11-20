package com.theok.kra.shell.payment.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

/** Selects the explicit access boundary for customer-facing payment endpoints. */
@ConfigurationProperties("payments.api-access")
data class PaymentApiAccessProperties(
    val mode: PaymentApiAccessMode = PaymentApiAccessMode.OIDC,
    val trustedDemoOwnerId: String = "local-demo-owner",
)

enum class PaymentApiAccessMode {
  TRUSTED_DEMO,
  OIDC,
}
