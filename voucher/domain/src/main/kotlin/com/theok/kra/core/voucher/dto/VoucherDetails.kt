package com.theok.kra.core.voucher.dto

import java.util.UUID

data class VoucherDetails(
    val id: UUID,
    val initialCapacity: Int,
    val remaining: Int,
)
