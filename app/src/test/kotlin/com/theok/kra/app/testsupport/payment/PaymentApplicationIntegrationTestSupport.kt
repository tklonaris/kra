package com.theok.kra.app.testsupport.payment

import com.stripe.StripeClient
import com.theok.kra.core.payment.dto.PaymentRequestAcceptance
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.commands.SubmitPaymentRequestResult
import com.theok.kra.shell.payment.crypto.ClientSecretCipher
import com.theok.kra.shell.payment.workflow.operations.PaymentLifecycleReconciliationOperations
import com.theok.kra.shell.payment.workflow.operations.PaymentWorkflowActivityOperations
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.DockerClientFactory

internal abstract class PaymentApplicationIntegrationTestSupport {
  protected val fixture = PaymentApplicationFixture()

  @BeforeAll
  fun startApplication() {
    assumeTrue(
        DockerClientFactory.instance().isDockerAvailable,
        "requires Docker for PostgreSQL Testcontainers",
    )
    fixture.start()
  }

  @AfterAll fun stopApplication() = fixture.close()

  @BeforeEach fun resetDatabase() = runBlocking { fixture.database.reset() }

  protected suspend fun acceptPaymentRequest(): PaymentRequestAcceptance =
      requireAccepted(
          fixture.commands.submitRequest(
              PaymentCommands.SubmitRequestSpec(
                  fixture.paymentOwner,
                  UUID.randomUUID(),
                  UUID.randomUUID(),
                  100,
                  "AUD",
              ),
          ),
      )

  protected fun requireAccepted(result: SubmitPaymentRequestResult): PaymentRequestAcceptance =
      (result as? SubmitPaymentRequestResult.Accepted)?.paymentRequest
          ?: error("Expected payment request to be accepted: $result")

  protected suspend fun registerPaymentForTest(): RegisteredPayment {
    val acceptedRequest = acceptPaymentRequest()
    return RegisteredPayment(
        acceptedRequest.paymentRequestId,
        registerPayment(acceptedRequest.paymentRequestId),
    )
  }

  protected suspend fun registerPayment(paymentRequestId: UUID): UUID =
      fixture.workflowTransactions.transaction {
        requireNotNull(registerPayment(paymentRequestId))
      }

  protected fun workflowOperations(stripe: StripeClient) =
      PaymentWorkflowActivityOperations(
          fixture.workflowTransactions,
          stripe,
          ClientSecretCipher(ByteArray(32)),
          PaymentLifecycleReconciliationOperations(fixture.workflowTransactions, stripe),
      )

  protected data class RegisteredPayment(
      val paymentRequestId: UUID,
      val paymentId: UUID,
  )
}
