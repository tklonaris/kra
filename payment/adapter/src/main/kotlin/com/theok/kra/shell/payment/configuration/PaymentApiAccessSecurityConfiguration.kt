package com.theok.kra.shell.payment.configuration

import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.shell.payment.controller.PaymentOwnerResolver
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.env.Environment
import org.springframework.security.config.Customizer
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.security.web.server.util.matcher.ServerWebExchangeMatchers

/** Feature-specific WebFlux policy: Stripe ingress is public; customer endpoints are not. */
@Configuration(proxyBeanMethods = false)
@EnableWebFluxSecurity
@Profile("payments")
class PaymentApiAccessSecurityConfiguration(
    private val properties: PaymentApiAccessProperties,
    environment: Environment,
) {
  init {
    if (
        properties.mode == PaymentApiAccessMode.TRUSTED_DEMO &&
            environment.activeProfiles.none { it in TRUSTED_DEMO_PROFILES }
    ) {
      error(
          "payments.api-access.mode=trusted-demo requires an explicit local or test Spring profile",
      )
    }
  }

  @Bean
  fun paymentOwnerResolver(): PaymentOwnerResolver =
      when (properties.mode) {
        PaymentApiAccessMode.TRUSTED_DEMO ->
            PaymentOwnerResolver { PaymentOwnerId(properties.trustedDemoOwnerId) }
        PaymentApiAccessMode.OIDC ->
            PaymentOwnerResolver { jwt ->
              val authenticatedJwt = requireNotNull(jwt) { "Authenticated JWT is required" }
              val issuer =
                  requireNotNull(authenticatedJwt.getClaimAsString("iss")) {
                    "JWT issuer is required"
                  }
              val subject =
                  requireNotNull(authenticatedJwt.getClaimAsString("sub")) {
                    "JWT subject is required"
                  }
              PaymentOwnerId("$issuer|$subject")
            }
      }

  @Bean
  fun paymentApiSecurityWebFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
    http
        .securityMatcher(
            ServerWebExchangeMatchers.pathMatchers(
                "/payment-requests/**",
                "/internal/payment-requests/**",
                "/payment-requests/stripe/webhook",
            ),
        )
        .csrf(ServerHttpSecurity.CsrfSpec::disable)
        .authorizeExchange { exchanges ->
          exchanges.pathMatchers("/payment-requests/stripe/webhook").permitAll()
          exchanges
              .pathMatchers("/internal/payment-requests/**")
              .hasAuthority("SCOPE_payments:retry")
          when (properties.mode) {
            PaymentApiAccessMode.TRUSTED_DEMO -> exchanges.anyExchange().permitAll()
            PaymentApiAccessMode.OIDC -> exchanges.anyExchange().authenticated()
          }
        }
    if (properties.mode == PaymentApiAccessMode.OIDC) {
      http.oauth2ResourceServer { resourceServer -> resourceServer.jwt(Customizer.withDefaults()) }
    }
    return http.build()
  }

  private companion object {
    val TRUSTED_DEMO_PROFILES = setOf("local", "test")
  }
}
