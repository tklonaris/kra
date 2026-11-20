package com.theok.kra.app.testsupport.voucher

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.platform.database.json.JooqJsonSupport
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import kotlin.test.assertEquals

/** HTTP test fixture for the Voucher controller contract. */
internal class VoucherHttpClient(
    private val applicationUrl: String,
    private val http: HttpClient = HttpClient.newHttpClient(),
    private val objectMapper: ObjectMapper = JooqJsonSupport.objectMapper(),
) {
  fun create(initialCapacity: Int = 1): Voucher {
    val response =
        http.send(
            HttpRequest.newBuilder(URI.create("$applicationUrl/vouchers"))
                .header("Content-Type", "application/json")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        objectMapper
                            .createObjectNode()
                            .put("initialCapacity", initialCapacity)
                            .toString(),
                    ),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    assertEquals(201, response.statusCode(), response.body())
    val json = objectMapper.readTree(response.body())
    return Voucher(UUID.fromString(json.required("id").asText()))
  }

  fun redeem(voucherId: UUID, requestId: UUID) {
    val response =
        http.send(
            HttpRequest.newBuilder(URI.create("$applicationUrl/vouchers/$voucherId/redemptions"))
                .header("Idempotency-Key", requestId.toString())
                .POST(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )
    assertEquals(200, response.statusCode(), response.body())
    val json = objectMapper.readTree(response.body())
    assertEquals("SUCCEEDED", json.required("status").asText())
    assertEquals(voucherId, UUID.fromString(json.required("voucherId").asText()))
    assertEquals(requestId, UUID.fromString(json.required("requestId").asText()))
  }

  data class Voucher(
      val id: UUID,
  )
}
