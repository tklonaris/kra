package com.theok.kra.app.payment

import com.theok.kra.app.testsupport.payment.PaymentWorkflowPersistenceTestSupport
import com.theok.kra.core.payment.domain.PaymentStatus
import com.theok.kra.core.payment.domain.TerminalReason
import com.theok.kra.shell.payment.persistence.coordination.PaymentStatusTransition
import com.theok.kra.shell.payment.persistence.lifecycle.PaymentLifecycleEventType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.TestInstance

/** Covers atomic, idempotent terminal payment transitions and their outbox records. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class PaymentTerminalTransitionPersistenceIntegrationTest :
    PaymentWorkflowPersistenceTestSupport() {
  @Test
  fun `workflow transaction rolls back a terminal transition and its outbox`() = runBlocking {
    val payment = registerPaymentWithKnownIntent()

    assertFailsWith<IllegalStateException> {
      fixture.workflowTransactions.transaction {
        updatePaymentStatus(payment.paymentId, PaymentStatus.REQUESTED, PaymentStatus.SUCCEEDED)
        error("force rollback after terminal transition")
      }
    }

    assertEquals(PaymentStatus.REQUESTED.name, paymentStatus(payment.paymentId))
    assertEquals(0, fixture.database.outboxCount(payment.paymentId, "PaymentSucceeded"))
  }

  @Test
  fun `terminal outbox conflict rolls back payment state`() = runBlocking {
    val payment = registerPaymentWithKnownIntent()
    fixture.database.insertTerminalOutbox(
        payment.paymentRequestId,
        payment.paymentId,
        "PaymentSucceeded",
    )

    assertFailsWith<Exception> {
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
    }

    assertEquals(PaymentStatus.REQUESTED.name, paymentStatus(payment.paymentId))
    assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentSucceeded"))
  }

  @Test
  fun `matching terminal transition is idempotent and does not duplicate its outbox event`() =
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

        val retry =
            fixture.workflowTransactions.transaction {
              updatePaymentStatus(
                  payment.paymentId,
                  PaymentStatus.REQUESTED,
                  PaymentStatus.SUCCEEDED,
              )
            }
        val conflictingRetry =
            fixture.workflowTransactions.transaction {
              updatePaymentStatus(
                  payment.paymentId,
                  PaymentStatus.REQUESTED,
                  PaymentStatus.CANCELLED,
                  TerminalReason.PROVIDER_CANCELLED,
              )
            }

        assertEquals(PaymentStatusTransition.MatchingTerminalOutcome, retry)
        assertEquals(
            PaymentStatusTransition.ConflictingTerminalOutcome(
                PaymentStatus.SUCCEEDED,
                PaymentStatus.CANCELLED,
            ),
            conflictingRetry,
        )
        assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentSucceeded"))
        assertEquals(
            1,
            fixture.database.lifecycleEventCount(
                payment.paymentId,
                PaymentLifecycleEventType.STATUS_CHANGED,
            ),
        )
      }

  @Test
  fun `concurrent terminal transitions append exactly one terminal outbox event`() = runBlocking {
    val payment = registerPaymentWithKnownIntent()

    val transitions: List<PaymentStatusTransition> = coroutineScope {
      List(2) {
            async<PaymentStatusTransition> {
              fixture.workflowTransactions.transaction<PaymentStatusTransition> {
                val transition =
                    updatePaymentStatus(
                        payment.paymentId,
                        PaymentStatus.REQUESTED,
                        PaymentStatus.SUCCEEDED,
                    )
                transition
              }
            }
          }
          .awaitAll()
    }

    assertEquals(1, transitions.count { it == PaymentStatusTransition.Updated })
    assertEquals(1, transitions.count { it == PaymentStatusTransition.MatchingTerminalOutcome })
    assertEquals(1, fixture.database.outboxCount(payment.paymentId, "PaymentSucceeded"))
  }
}
