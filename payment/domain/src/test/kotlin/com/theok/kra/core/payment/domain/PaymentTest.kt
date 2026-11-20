package com.theok.kra.core.payment.domain

import com.theok.kra.core.payment.dto.PaymentRequestSetup
import java.time.Instant
import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class PaymentTest {
  @Test
  fun `only AUD positive payment is valid`() {
    assertFailsWith<IllegalArgumentException> { PaymentSpec(UUID.randomUUID(), 0, "AUD") }
    assertFailsWith<IllegalArgumentException> { PaymentSpec(UUID.randomUUID(), 100, "USD") }
  }

  @Test
  fun `terminal status is recognised`() {
    assertTrue(PaymentStatus.SUCCEEDED.terminal)
    assertTrue(PaymentStatus.CANCELLED.terminal)
    assertFalse(PaymentStatus.PROCESSING.terminal)
  }

  @Test
  fun `checkout expiry applies only before payment processing`() {
    assertTrue(PaymentStatus.REQUESTED.checkoutExpiryApplies)
    assertTrue(PaymentStatus.AWAITING_CUSTOMER.checkoutExpiryApplies)
    assertFalse(PaymentStatus.PROCESSING.checkoutExpiryApplies)
    assertFalse(PaymentStatus.SUCCEEDED.checkoutExpiryApplies)
  }

  @Test
  fun `payment request status only permits lifecycle transitions`() {
    assertTrue(PaymentRequestStatus.ACCEPTED.canTransitionTo(PaymentRequestStatus.CANCELLED))
    assertTrue(
        PaymentRequestStatus.RECONCILING_PROVIDER_STATE.canTransitionTo(
            PaymentRequestStatus.WAITING_FOR_CUSTOMER,
        ),
    )
    assertFalse(
        PaymentRequestStatus.CREATING_PROVIDER_INTENT.canTransitionTo(
            PaymentRequestStatus.ACCEPTED,
        ),
    )
    assertFalse(
        PaymentRequestStatus.WAITING_FOR_PROCESSING_OUTCOME.canTransitionTo(
            PaymentRequestStatus.WAITING_FOR_CUSTOMER,
        ),
    )
  }

  @Test
  fun `provider status facts may advance a requested payment directly to processing`() {
    assertStatusChange(
        PaymentStatus.REQUESTED,
        PaymentStatus.PROCESSING,
        assertIs<PaymentStatusDecision.Change>(
                PaymentStatus.REQUESTED.providerStatus(PaymentStatusUpdate.PROCESSING),
            )
            .statusChange,
    )
  }

  @Test
  fun `status updates cannot regress a terminal payment`() {
    assertEquals(
        PaymentStatusDecision.NoChange,
        PaymentStatus.SUCCEEDED.providerStatus(PaymentStatusUpdate.PROCESSING),
    )
    assertEquals(
        PaymentStatusDecision.NoChange,
        PaymentStatus.SUCCEEDED.providerStatus(PaymentStatusUpdate.AWAITING_CUSTOMER),
    )
  }

  @Test
  fun `payment setup status is a pure lifecycle rule`() {
    val now = Instant.parse("2026-01-01T00:00:00Z")

    assertEquals(
        PaymentRequestSetupStatus.PENDING,
        paymentRequestSetupStatus(PaymentStatus.REQUESTED, now.plusSeconds(1), now),
    )
    assertEquals(
        PaymentRequestSetupStatus.AVAILABLE,
        paymentRequestSetupStatus(PaymentStatus.AWAITING_CUSTOMER, now.plusSeconds(1), now),
    )
    assertEquals(
        PaymentRequestSetupStatus.EXPIRED,
        paymentRequestSetupStatus(PaymentStatus.AWAITING_CUSTOMER, now, now),
    )
    assertEquals(
        PaymentRequestSetupStatus.UNAVAILABLE,
        paymentRequestSetupStatus(PaymentStatus.SUCCEEDED, now.plusSeconds(1), now),
    )
    assertEquals(
        PaymentRequestSetupStatus.UNAVAILABLE,
        paymentRequestSetupStatus(PaymentStatus.CANCELLED, now.plusSeconds(1), now),
    )
  }

  @Test
  fun `available setup requires a client secret`() {
    assertFailsWith<IllegalArgumentException> {
      PaymentRequestSetup(PaymentRequestSetupStatus.AVAILABLE)
    }
    assertFailsWith<IllegalArgumentException> {
      PaymentRequestSetup(PaymentRequestSetupStatus.PENDING, "secret")
    }
    assertEquals(
        "secret",
        PaymentRequestSetup(PaymentRequestSetupStatus.AVAILABLE, "secret").clientSecret,
    )
  }

  @Test
  fun `payment status centralises provider transition outcomes`() {
    assertChange(PaymentStatus.REQUESTED, PaymentStatusUpdate.AWAITING_CUSTOMER)
    assertChange(PaymentStatus.REQUESTED, PaymentStatusUpdate.PROCESSING)
    assertChange(PaymentStatus.AWAITING_CUSTOMER, PaymentStatusUpdate.SUCCEEDED)
    assertChange(PaymentStatus.PROCESSING, PaymentStatusUpdate.CANCELLED)
    assertEquals(
        PaymentStatusDecision.NoChange,
        PaymentStatus.PROCESSING.providerStatus(PaymentStatusUpdate.AWAITING_CUSTOMER),
    )
    assertEquals(
        PaymentStatusDecision.NoChange,
        PaymentStatus.SUCCEEDED.providerStatus(PaymentStatusUpdate.SUCCEEDED),
    )
    assertEquals(
        PaymentStatusDecision.ConflictingTerminalOutcome(
            PaymentStatus.SUCCEEDED,
            PaymentStatus.CANCELLED,
        ),
        PaymentStatus.SUCCEEDED.providerStatus(PaymentStatusUpdate.CANCELLED),
    )
  }

  @Test
  fun `lifecycle state owns intent-created and local-cancellation edges`() {
    assertStatusChange(
        PaymentStatus.REQUESTED,
        PaymentStatus.AWAITING_CUSTOMER,
        PaymentStatus.REQUESTED.intentCreated(),
    )
    assertFailsWith<IllegalStateException> {
      PaymentStatus.PROCESSING.intentCreated()
    }
    assertStatusChange(
        PaymentStatus.REQUESTED,
        PaymentStatus.CANCELLED,
        PaymentLifecycleState(
                status = PaymentStatus.REQUESTED,
                intentCreationState = IntentCreationState.NOT_STARTED,
                providerIntentRecorded = false,
                cancellationRequested = true,
                cancellationOrigin = TerminalReason.CUSTOMER_CANCELLED,
            )
            .cancelWithoutProviderIntent(),
    )
    assertFailsWith<IllegalStateException> {
      PaymentLifecycleState(
              status = PaymentStatus.PROCESSING,
              intentCreationState = IntentCreationState.KNOWN,
              providerIntentRecorded = true,
              cancellationRequested = false,
              cancellationOrigin = null,
          )
          .cancelWithoutProviderIntent()
    }
    assertFailsWith<IllegalStateException> {
      PaymentLifecycleState(
              status = PaymentStatus.REQUESTED,
              intentCreationState = IntentCreationState.ATTEMPTING,
              providerIntentRecorded = false,
              cancellationRequested = false,
              cancellationOrigin = null,
          )
          .cancelWithoutProviderIntent()
    }
  }

  @Test
  fun `cancellation state resolves only determinate outcomes after a local claim`() {
    val state =
        PaymentLifecycleState(
            status = PaymentStatus.AWAITING_CUSTOMER,
            intentCreationState = IntentCreationState.KNOWN,
            providerIntentRecorded = true,
            cancellationRequested = true,
            cancellationOrigin = TerminalReason.CUSTOMER_CANCELLED,
        )

    assertTrue(
        state.cancellationIsResolvedBy(
            PaymentStatus.CANCELLED,
            TerminalReason.CUSTOMER_CANCELLED,
        ),
    )
    assertTrue(state.cancellationIsResolvedBy(PaymentStatus.SUCCEEDED, null))
    assertFalse(
        state.cancellationIsResolvedBy(
            PaymentStatus.CANCELLED,
            TerminalReason.CAUSE_UNDETERMINED,
        ),
    )
    assertFalse(
        PaymentLifecycleState(
                status = PaymentStatus.AWAITING_CUSTOMER,
                intentCreationState = IntentCreationState.KNOWN,
                providerIntentRecorded = true,
                cancellationRequested = true,
                cancellationOrigin = null,
            )
            .cancellationIsResolvedBy(
                PaymentStatus.CANCELLED,
                TerminalReason.PROVIDER_CANCELLED,
            ),
    )
  }

  @Test
  fun `cancellation reason policy stays with the reason`() {
    assertTrue(TerminalReason.CUSTOMER_CANCELLED.isLocalCancellation)
    assertTrue(TerminalReason.CHECKOUT_EXPIRED.isLocalCancellation)
    assertFalse(TerminalReason.PROVIDER_CANCELLED.isLocalCancellation)
    assertTrue(TerminalReason.PROVIDER_CANCELLED.resolvesUndeterminedCancellation)
    assertFalse(TerminalReason.CAUSE_UNDETERMINED.resolvesUndeterminedCancellation)
  }

  private fun assertChange(
      current: PaymentStatus,
      update: PaymentStatusUpdate,
  ) {
    assertStatusChange(
        current,
        PaymentStatus.valueOf(update.name),
        assertIs<PaymentStatusDecision.Change>(current.providerStatus(update)).statusChange,
    )
  }

  private fun assertStatusChange(
      expectedStatus: PaymentStatus,
      nextStatus: PaymentStatus,
      actual: PaymentStatusChange,
  ) {
    assertEquals(expectedStatus, actual.expectedStatus)
    assertEquals(nextStatus, actual.nextStatus)
  }
}
