package com.theok.kra.shell.payment.configuration

import com.theok.kra.shell.payment.configuration.stripe.PaymentStripeProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

/**
 * Registers payment feature settings independently of optional Stripe and Temporal infrastructure.
 */
@Configuration(proxyBeanMethods = false)
@Profile("payments")
@EnableConfigurationProperties(
    PaymentStripeProperties::class,
    PaymentTemporalProperties::class,
    PaymentApiAccessProperties::class,
)
class PaymentPropertiesConfiguration
