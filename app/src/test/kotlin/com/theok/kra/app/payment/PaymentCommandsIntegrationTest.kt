package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentDatabaseFixture.PaymentRequestLifecycleTransition
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.app.testsupport.payment.PaymentWorkflowStripeFake
import com.theok.kra.core.payment.domain.PaymentRequestRetryResult
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.commands.CancelPaymentResult
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Verifies request commands and their transactional request/payment outcomes. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentCommandsIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `creation is idempotent and emits one workflow event`() = runBlocking {
    val request =
        PaymentCommands.SubmitRequestSpec(
            fixture.paymentOwner,
            UUID.randomUUID(),
            UUID.randomUUID(),
            100,
            "AUD",
        )

    val first = fixture.commands.submitRequest(request)
    val retry = fixture.commands.submitRequest(request)

    assertEquals(first, retry)
    val paymentRequestId = requireAccepted(first).paymentRequestId
    assertEquals(
        1,
        fixture.database.paymentRequestOutboxCount(paymentRequestId, "PaymentRequestAccepted"),
    )
    assertEquals(
        1,
        fixture.database.paymentRequestLifecycleEventCount(paymentRequestId, "ACCEPTED"),
    )
  }

  @Test
  fun `creation retry returns its original accepted outcome after payment registration`() =
      runBlocking {
        val request =
            PaymentCommands.SubmitRequestSpec(
                fixture.paymentOwner,
                UUID.randomUUID(),
                UUID.randomUUID(),
                100,
                "AUD",
            )
        val accepted = fixture.commands.submitRequest(request)
        val paymentRequestId = requireAccepted(accepted).paymentRequestId
        val paymentId = registerPayment(paymentRequestId)

        assertEquals(accepted, fixture.commands.submitRequest(request))
        assertEquals(
            1,
            fixture.database.lifecycleEventCount(
                paymentId,
                PaymentLifecycleEventType.PAYMENT_CREATED,
            ),
        )
      }

  @Test
  fun `cancellation is idempotent and distinguishes terminal and missing requests`() = runBlocking {
    val payment = registerPaymentForTest()

    assertEquals(
        CancelPaymentResult.Requested,
        fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId),
    )
    assertEquals(
        CancelPaymentResult.AlreadyRequested,
        fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId),
    )
    assertEquals(
        1,
        fixture.database.paymentRequestOutboxCount(
            payment.paymentRequestId,
            "PaymentCancellationRequested",
        ),
    )
    assertEquals(
        1,
        fixture.database.paymentRequestLifecycleEventCount(
            payment.paymentRequestId,
            "CANCELLATION_REQUESTED",
        ),
    )
    assertEquals(
        PaymentRequestStatus.RECONCILING_CANCELLATION.name,
        fixture.database.paymentRequestStatus(payment.paymentRequestId),
    )
    assertEquals(
        PaymentRequestLifecycleTransition(
            PaymentRequestStatus.CREATING_PROVIDER_INTENT.name,
            PaymentRequestStatus.RECONCILING_CANCELLATION.name,
            "CUSTOMER_CANCELLED",
        ),
        fixture.database.paymentRequestLifecycleTransition(
            payment.paymentRequestId,
            "CANCELLATION_REQUESTED",
        ),
    )

    val terminalPayment = registerPaymentWithKnownIntent()
    fixture.workflowTransactions.transaction {
      assertEquals(
          PaymentStatusTransition.Updated,
          updatePaymentStatus(
              terminalPayment.paymentId,
              PaymentStatus.REQUESTED,
              PaymentStatus.SUCCEEDED,
          ),
      )
    }
    assertEquals(
        CancelPaymentResult.Terminal,
        fixture.commands.cancel(fixture.paymentOwner, terminalPayment.paymentRequestId),
    )
    assertEquals(
        CancelPaymentResult.NotFound,
        fixture.commands.cancel(fixture.paymentOwner, UUID.randomUUID()),
    )
  }

  @Test
  fun `cancellation and provider creation claims race through request-first locking`() =
      runBlocking {
        repeat(5) {
          val payment = registerPaymentForTest()
          val start = CompletableDeferred<Unit>()
          coroutineScope {
            val cancellation =
                async(Dispatchers.Default) {
                  start.await()
                  fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId)
                }
            val creationClaim =
                async(Dispatchers.Default) {
                  start.await()
                  fixture.workflowTransactions.transaction {
                    claimIntentCreation(
                        payment.paymentId,
                        UUID.randomUUID(),
                        "claim-key-${UUID.randomUUID()}",
                        "100:AUD:automatic-card",
                    )
                  }
                }
            start.complete(Unit)

            assertEquals(CancelPaymentResult.Requested, cancellation.await())
            creationClaim.await()
          }
          assertEquals(
              PaymentRequestStatus.RECONCILING_CANCELLATION.name,
              fixture.database.paymentRequestStatus(payment.paymentRequestId),
          )
        }
      }

  @Test
  fun `request-only activity failures become retryable without a payment`() = runBlocking {
    val request = acceptPaymentRequest()

    val control =
        workflowOperations(PaymentWorkflowStripeFake().client)
            .requireReconciliation(request.paymentRequestId, "registration retries exhausted")
    workflowOperations(PaymentWorkflowStripeFake().client)
        .markOperationalFailure(request.paymentRequestId, "registration retries exhausted")

    assertTrue(control.complete)
    assertEquals(0, fixture.database.paymentCount(request.paymentRequestId))
    assertEquals(
        PaymentRequestStatus.FAILED.name,
        fixture.database.paymentRequestStatus(request.paymentRequestId),
    )
    assertEquals(
        1,
        fixture.database.paymentRequestLifecycleEventCount(
            request.paymentRequestId,
            "OPERATIONAL_FAILURE",
        ),
    )
    assertEquals(
        PaymentRequestRetryResult.ACCEPTED,
        fixture.commands.retry(
            request.paymentRequestId,
            "payment-operator",
            "registration dependency restored",
        ),
    )
    registerPayment(request.paymentRequestId)
    assertEquals(1, fixture.database.paymentCount(request.paymentRequestId))
  }

  @Test
  fun `failed request without a payment can still be cancelled`() = runBlocking {
    val request = acceptPaymentRequest()
    workflowOperations(PaymentWorkflowStripeFake().client)
        .markOperationalFailure(request.paymentRequestId, "registration retries exhausted")

    assertEquals(
        CancelPaymentResult.Requested,
        fixture.commands.cancel(fixture.paymentOwner, request.paymentRequestId),
    )
    assertEquals(0, fixture.database.paymentCount(request.paymentRequestId))
    assertEquals(
        PaymentRequestStatus.CANCELLED.name,
        fixture.database.paymentRequestStatus(request.paymentRequestId),
    )
  }

  @Test
  fun `cancelling a failed request with a terminal payment remains terminal`() = runBlocking {
    listOf(PaymentStatus.SUCCEEDED, PaymentStatus.CANCELLED).forEach { terminalStatus ->
      val payment = registerPaymentWithKnownIntent()
      fixture.workflowTransactions.transaction {
        updatePaymentStatus(
            payment.paymentId,
            PaymentStatus.REQUESTED,
            terminalStatus,
            if (terminalStatus == PaymentStatus.CANCELLED) TerminalReason.PROVIDER_CANCELLED
            else null,
        )
        markOperationalFailure(payment.paymentId, "post-terminal operational failure")
      }

      assertEquals(
          CancelPaymentResult.Terminal,
          fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId),
      )
      assertEquals(
          PaymentRequestStatus.FAILED.name,
          fixture.database.paymentRequestStatus(payment.paymentRequestId),
      )
      assertEquals(
          0,
          fixture.database.paymentRequestOutboxCount(
              payment.paymentRequestId,
              "PaymentCancellationRequested",
          ),
      )
    }
  }
}
