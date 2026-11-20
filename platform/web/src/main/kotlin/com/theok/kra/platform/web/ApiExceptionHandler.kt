package com.theok.kra.platform.web

import java.net.URI
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.HttpStatusCode
import org.springframework.http.ProblemDetail
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebInputException

/** Maps exceptional HTTP failures to the API's stable problem-response contract. */
@RestControllerAdvice
class ApiExceptionHandler {
  @ExceptionHandler(ServerWebInputException::class)
  fun invalidRequest(exception: ServerWebInputException): ProblemDetail =
      problem(
          HttpStatus.BAD_REQUEST,
          "invalid-request",
          "Invalid request",
          exception.reason ?: "Request input is invalid",
      )

  @ExceptionHandler(ResponseStatusException::class)
  fun responseStatus(exception: ResponseStatusException): ProblemDetail {
    val (code, title) =
        when (exception.statusCode) {
          HttpStatus.NOT_FOUND -> "resource-not-found" to "Resource not found"
          else -> "request-failed" to "Request failed"
        }
    return problem(
        exception.statusCode,
        code,
        title,
        exception.reason ?: "The request could not be completed",
    )
  }

  @ExceptionHandler(Exception::class)
  fun internalError(exception: Exception): ProblemDetail {
    logger.error("Unhandled API exception", exception)
    return problem(
        HttpStatus.INTERNAL_SERVER_ERROR,
        "internal-error",
        "Internal server error",
        "An unexpected error occurred",
    )
  }

  private fun problem(
      status: HttpStatusCode,
      code: String,
      title: String,
      detail: String,
  ): ProblemDetail =
      ProblemDetail.forStatusAndDetail(status, detail).apply {
        type = URI.create("urn:kra:problem:$code")
        this.title = title
        setProperty("code", code)
      }

  private companion object {
    val logger = LoggerFactory.getLogger(ApiExceptionHandler::class.java)
  }
}
