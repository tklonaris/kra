package com.theok.kra.shell.voucher.persistence.entity

import java.util.UUID
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table

@Table("voucher")
data class VoucherEntity(
    @Id val id: UUID,
    val initialCapacity: Int,
    val remaining: Int,
)
