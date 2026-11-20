package com.theok.kra.shell.payment.crypto

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class ClientSecretCipherTest {
  @Test
  fun `uses fresh nonce and authenticates payment binding`() {
    val cipher = ClientSecretCipher(ByteArray(32) { it.toByte() })
    val first = cipher.encrypt("payment-a", "pi_secret")
    val second = cipher.encrypt("payment-a", "pi_secret")
    assertFalse(first.nonce.contentEquals(second.nonce))
    assertEquals("pi_secret", cipher.decrypt("payment-a", first))
    assertFailsWith<Exception> { cipher.decrypt("payment-b", first) }
  }
}
