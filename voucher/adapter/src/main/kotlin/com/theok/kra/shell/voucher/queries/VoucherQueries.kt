package com.theok.kra.shell.voucher.queries

import com.theok.kra.core.voucher.dto.VoucherDetails
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.voucher.generated.jooq.Tables.VOUCHER
import java.util.UUID
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux

@Component
class VoucherQueries(
    private val jooq: TransactionalJooqExecutor,
) {
  suspend fun details(id: UUID): VoucherDetails? = jooq.execute { dsl ->
    Flux.from(
            dsl.select(VOUCHER.ID, VOUCHER.INITIAL_CAPACITY, VOUCHER.REMAINING)
                .from(VOUCHER)
                .where(VOUCHER.ID.eq(id)),
        )
        .map { row -> row.into(VoucherDetails::class.java) }
        .singleOrEmpty()
        .awaitSingleOrNull()
  }
}
