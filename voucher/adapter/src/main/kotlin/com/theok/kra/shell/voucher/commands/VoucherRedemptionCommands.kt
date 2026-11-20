package com.theok.kra.shell.voucher.commands

import com.theok.kra.core.voucher.domain.RedemptionOutcome
import com.theok.kra.shell.voucher.persistence.VoucherRedemptionPersister
import java.util.UUID
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

/** Defines the redemption write unit and its Spring-managed transaction boundary. */
@Component
class VoucherRedemptionCommands(
    private val redemptionPersister: VoucherRedemptionPersister,
) {
  data class RedeemSpec(
      val requestId: UUID,
      val voucherId: UUID,
  )

  @Transactional
  suspend fun redeem(command: RedeemSpec): RedemptionOutcome =
      redemptionPersister.redeemAtomically(command.requestId, command.voucherId)
}
