package com.theok.kra.core.voucher.domain

import java.util.UUID

data class Voucher(
    val id: UUID,
    val initialCapacity: Int,
    val remaining: Int,
) {
  init {
    require(initialCapacity > 0) { "Voucher capacity must be positive" }
    require(remaining in 0..initialCapacity) { "Voucher remaining capacity is invalid" }
  }

  companion object {
    fun create(id: UUID, initialCapacity: Int): Voucher =
        Voucher(id = id, initialCapacity = initialCapacity, remaining = initialCapacity)
  }
}
