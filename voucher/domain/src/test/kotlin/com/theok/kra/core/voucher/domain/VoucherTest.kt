package com.theok.kra.core.voucher.domain

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class VoucherTest {
  @Test
  fun `creates a voucher with all capacity remaining`() {
    val voucher = Voucher.create(UUID.randomUUID(), 3)

    assertEquals(3, voucher.initialCapacity)
    assertEquals(3, voucher.remaining)
  }

  @Test
  fun `rejects non-positive capacity`() {
    assertFailsWith<IllegalArgumentException> { Voucher.create(UUID.randomUUID(), 0) }
    assertFailsWith<IllegalArgumentException> { Voucher.create(UUID.randomUUID(), -1) }
  }
}
