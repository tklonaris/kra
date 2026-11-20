package com.theok.kra.core.voucher.domain

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertFailsWith

class RedemptionOutcomeTest {
  private val requestId = UUID.randomUUID()
  private val voucherId = UUID.randomUUID()

  @Test
  fun `succeeded outcome cannot carry a rejection reason`() {
    assertFailsWith<IllegalArgumentException> {
      RedemptionOutcome(
          requestId,
          voucherId,
          RedemptionStatus.SUCCEEDED,
          RejectionReason.CAPACITY_EXHAUSTED,
      )
    }
  }

  @Test
  fun `rejected outcome requires a rejection reason`() {
    assertFailsWith<IllegalArgumentException> {
      RedemptionOutcome(requestId, voucherId, RedemptionStatus.REJECTED)
    }
  }
}
