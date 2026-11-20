package com.theok.kra.platform.web

import kotlin.test.Test
import kotlin.test.assertEquals
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebInputException

class ApiExceptionHandlerTest {
  private val handler = ApiExceptionHandler()

  @Test
  fun `invalid input returns a stable bad request problem`() {
    val problem = handler.invalidRequest(ServerWebInputException("Invalid UUID"))

    assertEquals(HttpStatus.BAD_REQUEST.value(), problem.status)
    assertEquals("urn:kra:problem:invalid-request", problem.type.toString())
    assertEquals("invalid-request", problem.properties?.get("code"))
  }

  @Test
  fun `response status preserves missing resource semantics`() {
    val problem =
        handler.responseStatus(ResponseStatusException(HttpStatus.NOT_FOUND, "Voucher not found"))

    assertEquals(HttpStatus.NOT_FOUND.value(), problem.status)
    assertEquals("urn:kra:problem:resource-not-found", problem.type.toString())
    assertEquals("Voucher not found", problem.detail)
  }

  @Test
  fun `unexpected errors do not expose exception details`() {
    val problem = handler.internalError(IllegalStateException("database password leaked"))

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.value(), problem.status)
    assertEquals("urn:kra:problem:internal-error", problem.type.toString())
    assertEquals("An unexpected error occurred", problem.detail)
  }
}
