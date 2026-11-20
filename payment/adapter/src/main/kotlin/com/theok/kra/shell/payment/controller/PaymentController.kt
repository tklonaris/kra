package com.theok.kra.shell.payment.controller

import com.theok.kra.core.payment.domain.PaymentRequestRetryResult
import com.theok.kra.core.payment.domain.PaymentRequestSetupStatus
import com.theok.kra.core.payment.dto.PaymentRequestAcceptance
import com.theok.kra.core.payment.dto.PaymentRequestSnapshot
import com.theok.kra.shell.payment.commands.CancelPaymentResult
import com.theok.kra.shell.payment.commands.InvalidStripeWebhook
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.commands.PaymentWebhookCommands
import com.theok.kra.shell.payment.commands.SubmitPaymentRequestResult
import com.theok.kra.shell.payment.queries.PaymentQueries
import jakarta.validation.Valid
import jakarta.validation.constraints.Positive
import java.util.UUID
import org.springframework.context.annotation.Profile
import org.springframework.http.CacheControl
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@Profile("payments")
@RequestMapping("/payment-requests")
class PaymentController(
    private val commands: PaymentCommands,
    private val queries: PaymentQueries,
    private val webhookCommands: PaymentWebhookCommands,
    private val ownerResolver: PaymentOwnerResolver,
) {
  @PostMapping
  suspend fun createRequest(
      @RequestHeader("Idempotency-Key") requestId: UUID,
      @Valid @RequestBody request: CreatePaymentRequest,
      @AuthenticationPrincipal jwt: Jwt?,
  ): ResponseEntity<PaymentRequestAcceptance> =
      when (
          val result =
              commands.submitRequest(
                  PaymentCommands.SubmitRequestSpec(
                      ownerResolver.resolve(jwt),
                      requestId,
                      request.purchaseReference,
                      request.amountMinor,
                      request.currency,
                  ),
              )
      ) {
        is SubmitPaymentRequestResult.Accepted ->
            ResponseEntity.status(HttpStatus.ACCEPTED).body(result.paymentRequest)
        SubmitPaymentRequestResult.Conflict ->
            throw ResponseStatusException(
                HttpStatus.CONFLICT,
                "Idempotency key or purchase reference is already associated with different input",
            )
        is SubmitPaymentRequestResult.Invalid ->
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, result.message)
      }

  @GetMapping("/{paymentRequestId}")
  suspend fun requestDetails(
      @PathVariable("paymentRequestId") paymentRequestId: UUID,
      @AuthenticationPrincipal jwt: Jwt?,
  ): PaymentRequestSnapshot =
      queries.requestDetails(ownerResolver.resolve(jwt), paymentRequestId)
          ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found")

  @GetMapping("/{paymentRequestId}/setup")
  suspend fun setup(
      @PathVariable("paymentRequestId") paymentRequestId: UUID,
      @AuthenticationPrincipal jwt: Jwt?,
  ): ResponseEntity<PaymentRequestSetupResponse> {
    val ownerId = ownerResolver.resolve(jwt)
    val setup =
        queries.requestSetup(ownerId, paymentRequestId)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found")
    return when (setup.status) {
      PaymentRequestSetupStatus.EXPIRED ->
          throw ResponseStatusException(HttpStatus.GONE, "Payment setup has expired")
      PaymentRequestSetupStatus.UNAVAILABLE ->
          throw ResponseStatusException(HttpStatus.CONFLICT, "Payment setup is unavailable")
      PaymentRequestSetupStatus.PENDING ->
          ResponseEntity.status(HttpStatus.ACCEPTED).cacheControl(CacheControl.noStore()).build()
      PaymentRequestSetupStatus.AVAILABLE ->
          ResponseEntity.ok()
              .cacheControl(CacheControl.noStore())
              .body(PaymentRequestSetupResponse(requireNotNull(setup.clientSecret)))
    }
  }

  @PostMapping("/{paymentRequestId}/cancellations")
  suspend fun cancel(
      @PathVariable("paymentRequestId") paymentRequestId: UUID,
      @AuthenticationPrincipal jwt: Jwt?,
  ): ResponseEntity<PaymentRequestSnapshot> {
    val ownerId = ownerResolver.resolve(jwt)
    return when (commands.cancel(ownerId, paymentRequestId)) {
      CancelPaymentResult.Requested,
      CancelPaymentResult.AlreadyRequested ->
          ResponseEntity.status(HttpStatus.ACCEPTED)
              .body(requireNotNull(queries.requestDetails(ownerId, paymentRequestId)))
      CancelPaymentResult.Terminal ->
          throw ResponseStatusException(HttpStatus.CONFLICT, "Payment is already terminal")
      CancelPaymentResult.NotFound ->
          throw ResponseStatusException(HttpStatus.NOT_FOUND, "Payment not found")
    }
  }

  @PostMapping("/stripe/webhook")
  suspend fun stripeWebhook(
      @RequestHeader("Stripe-Signature") signature: String,
      @RequestBody body: String,
  ): ResponseEntity<Void> =
      try {
        webhookCommands.accept(body, signature)
        ResponseEntity.ok().build()
      } catch (_: InvalidStripeWebhook) {
        throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid Stripe webhook")
      }
}

@RestController
@Profile("payments")
@RequestMapping("/internal/payment-requests")
class PaymentRetryController(private val commands: PaymentCommands) {
  @PostMapping("/{paymentRequestId}/retries")
  suspend fun retry(
      @PathVariable("paymentRequestId") paymentRequestId: UUID,
      @Valid @RequestBody request: PaymentRetryRequest,
      @AuthenticationPrincipal jwt: Jwt?,
  ): ResponseEntity<Void> {
    val subject = requireNotNull(jwt?.subject) { "Authenticated JWT subject is required" }
    return when (commands.retry(paymentRequestId, subject, request.reason)) {
      PaymentRequestRetryResult.ACCEPTED -> ResponseEntity.accepted().build()
      PaymentRequestRetryResult.NOT_FOUND ->
          throw ResponseStatusException(HttpStatus.NOT_FOUND, "Payment request not found")
      PaymentRequestRetryResult.NOT_ELIGIBLE ->
          throw ResponseStatusException(
              HttpStatus.CONFLICT,
              "Payment request is not eligible for retry",
          )
    }
  }
}

data class CreatePaymentRequest(
    val purchaseReference: UUID,
    @field:Positive val amountMinor: Long,
    val currency: String,
)

data class PaymentRequestSetupResponse(
    val clientSecret: String,
)

data class PaymentRetryRequest(
    @field:jakarta.validation.constraints.Size(max = 512) val reason: String,
)
