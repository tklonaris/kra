package com.theok.kra.shell.payment.crypto

import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.spec.GCMParameterSpec
import javax.crypto.spec.SecretKeySpec

data class EncryptedClientSecret(
    val nonce: ByteArray,
    val ciphertext: ByteArray,
    val tag: ByteArray,
)

/** AES-256-GCM envelope. The payment ID makes ciphertext non-transferable between rows. */
class ClientSecretCipher(private val key: ByteArray) {
  private val random = SecureRandom()

  fun encrypt(paymentId: String, plainText: String): EncryptedClientSecret {
    val nonce = ByteArray(NONCE_BYTES).also(random::nextBytes)
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(Cipher.ENCRYPT_MODE, SecretKeySpec(key, "AES"), GCMParameterSpec(TAG_BITS, nonce))
    cipher.updateAAD(paymentId.toByteArray(Charsets.UTF_8))
    val combined = cipher.doFinal(plainText.toByteArray(Charsets.UTF_8))
    return EncryptedClientSecret(
        nonce,
        combined.copyOfRange(0, combined.size - TAG_BYTES),
        combined.copyOfRange(combined.size - TAG_BYTES, combined.size),
    )
  }

  fun decrypt(paymentId: String, encrypted: EncryptedClientSecret): String {
    val cipher = Cipher.getInstance("AES/GCM/NoPadding")
    cipher.init(
        Cipher.DECRYPT_MODE,
        SecretKeySpec(key, "AES"),
        GCMParameterSpec(TAG_BITS, encrypted.nonce),
    )
    cipher.updateAAD(paymentId.toByteArray(Charsets.UTF_8))
    return String(cipher.doFinal(encrypted.ciphertext + encrypted.tag), Charsets.UTF_8)
  }

  private companion object {
    const val NONCE_BYTES = 12
    const val TAG_BYTES = 16
    const val TAG_BITS = 128
  }
}
