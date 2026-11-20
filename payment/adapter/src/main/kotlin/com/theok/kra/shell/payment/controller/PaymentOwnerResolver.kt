package com.theok.kra.shell.payment.controller

import com.theok.kra.core.payment.domain.PaymentOwnerId
import org.springframework.security.oauth2.jwt.Jwt

/** Resolves the HTTP caller to the framework-free owner value passed to payment operations. */
fun interface PaymentOwnerResolver {
  fun resolve(jwt: Jwt?): PaymentOwnerId
}
