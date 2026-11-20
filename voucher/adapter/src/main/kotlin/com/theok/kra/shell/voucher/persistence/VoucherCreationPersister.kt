package com.theok.kra.shell.voucher.persistence

import com.theok.kra.core.voucher.domain.Voucher
import com.theok.kra.shell.voucher.persistence.entity.VoucherEntity
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.stereotype.Component

@Component
class VoucherCreationPersister(
    private val template: R2dbcEntityTemplate,
) {
  suspend fun create(voucher: Voucher): Voucher {
    template
        .insert(VoucherEntity(voucher.id, voucher.initialCapacity, voucher.remaining))
        .awaitSingle()
    return voucher
  }
}
