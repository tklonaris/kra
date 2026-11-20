package com.theok.kra.core.voucher.domain

import java.util.UUID

data class RedemptionOutcome(
    val requestId: UUID,
    val voucherId: UUID,
    val status: RedemptionStatus,
    val rejectionReason: RejectionReason? = null,
) {
  init {
    require((status == RedemptionStatus.SUCCEEDED) == (rejectionReason == null)) {
      "A succeeded redemption has no rejection reason; a rejected redemption has one"
    }
  }
}

enum class RedemptionStatus {
  SUCCEEDED,
  REJECTED,
}

enum class RejectionReason {
  CAPACITY_EXHAUSTED,
  VOUCHER_NOT_FOUND,
  IDEMPOTENCY_CONFLICT,
}
