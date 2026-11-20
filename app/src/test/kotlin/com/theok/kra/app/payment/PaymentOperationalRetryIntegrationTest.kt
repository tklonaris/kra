package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentDatabaseFixture.PaymentRequestLifecycleTransition
import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.core.payment.domain.PaymentRequestRetryResult
import com.theok.kra.core.payment.domain.PaymentRequestStatus
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.shell.payment.commands.CancelPaymentResult
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Covers authorised retry after an operational payment-request failure. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentOperationalRetryIntegrationTest : PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `operational retry re-enters a failed request without changing a succeeded payment`() =
      runBlocking {
        val payment = registerPaymentWithKnownIntent()
        fixture.workflowTransactions.transaction {
          assertEquals(
              PaymentStatusTransition.Updated,
              updatePaymentStatus(
                  payment.paymentId,
                  PaymentStatus.REQUESTED,
                  PaymentStatus.SUCCEEDED,
              ),
          )
        }
        fixture.workflowTransactions.transaction {
          markOperationalFailure(payment.paymentId, "Conflicting terminal payment outcome")
        }

        assertEquals(PaymentStatus.SUCCEEDED.name, paymentStatus(payment.paymentId))
        assertEquals(
            PaymentRequestStatus.FAILED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
        assertEquals(
            PaymentRequestLifecycleTransition(
                PaymentRequestStatus.COMPLETED.name,
                PaymentRequestStatus.FAILED.name,
                "Conflicting terminal payment outcome",
            ),
            fixture.database.paymentRequestLifecycleTransition(
                payment.paymentRequestId,
                "OPERATIONAL_FAILURE",
            ),
        )
        assertEquals(
            PaymentRequestRetryResult.ACCEPTED,
            fixture.commands.retry(
                payment.paymentRequestId,
                "payment-operator",
                "reviewed conflict",
            ),
        )
        assertEquals(payment.paymentId, registerPayment(payment.paymentRequestId))
        assertEquals(
            PaymentRequestStatus.COMPLETED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
      }

  @Test
  fun `operational retry completes a terminal payment despite a historical cancellation request`() =
      runBlocking {
        val payment = registerPaymentWithKnownIntent()
        assertEquals(
            CancelPaymentResult.Requested,
            fixture.commands.cancel(fixture.paymentOwner, payment.paymentRequestId),
        )
        fixture.workflowTransactions.transaction {
          updatePaymentStatus(payment.paymentId, PaymentStatus.REQUESTED, PaymentStatus.SUCCEEDED)
          markOperationalFailure(payment.paymentId, "post-terminal operational failure")
        }

        assertEquals(
            PaymentRequestStatus.FAILED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
        assertEquals(
            PaymentRequestRetryResult.ACCEPTED,
            fixture.commands.retry(
                payment.paymentRequestId,
                "payment-operator",
                "reviewed conflict",
            ),
        )
        assertEquals(payment.paymentId, registerPayment(payment.paymentRequestId))
        assertEquals(
            PaymentRequestStatus.COMPLETED.name,
            fixture.database.paymentRequestStatus(payment.paymentRequestId),
        )
      }
}
