package com.theok.kra.platform.database.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import org.jooq.JSON
import org.jooq.JSONB

class JooqJsonSupportTest {
  private val node = JsonNodeFactory.instance.objectNode().put("name", "voucher")

  @Test
  fun `converts JSONB to and from a JSON node`() {
    val converter: org.jooq.Converter<JSONB, JsonNode> =
        JooqJsonConverterProvider(JooqJsonSupport.objectMapper())
            .provide(JSONB::class.java, JsonNode::class.java)

    assertEquals(node, converter.from(JSONB.jsonb("{\"name\":\"voucher\"}")))
    assertEquals("{\"name\":\"voucher\"}", converter.to(node)?.data())
    assertNull(converter.from(null))
    assertNull(converter.to(null))
  }

  @Test
  fun `serializes jooq JSON values as structured JSON`() {
    val mapper = JooqJsonSupport.objectMapper().registerModule(JooqJsonSupport.jacksonModule())

    assertEquals(
        "{\"name\":\"voucher\"}",
        mapper.writeValueAsString(JSON.json("{\"name\":\"voucher\"}")),
    )
    assertEquals(
        "{\"name\":\"voucher\"}",
        mapper.writeValueAsString(JSONB.jsonb("{\"name\":\"voucher\"}")),
    )
  }
}
