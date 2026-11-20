package com.theok.kra.shell.voucher.commands

import com.theok.kra.core.voucher.domain.Voucher
import com.theok.kra.core.voucher.dto.VoucherDetails
import com.theok.kra.shell.voucher.persistence.VoucherCreationPersister
import java.util.UUID
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
open class VoucherCreationCommands(
    private val voucherCreationPersister: VoucherCreationPersister,
) {
  data class CreateSpec(
      val initialCapacity: Int,
  )

  @Transactional
  open suspend fun create(command: CreateSpec): VoucherDetails {
    val voucher = Voucher.create(UUID.randomUUID(), command.initialCapacity)
    voucherCreationPersister.create(voucher)
    return VoucherDetails(voucher.id, voucher.initialCapacity, voucher.remaining)
  }
}
