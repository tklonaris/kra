package com.theok.kra.platform.database.json

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.JsonSerializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import java.io.IOException
import org.jooq.Converter
import org.jooq.ConverterProvider
import org.jooq.JSON
import org.jooq.JSONB
import org.jooq.exception.DataTypeException
import org.jooq.impl.AbstractConverter
import org.jooq.impl.DefaultConverterProvider

/** Shared JSON configuration for generated jOOQ values and WebFlux serialization. */
object JooqJsonSupport {
  fun objectMapper(): ObjectMapper =
      JsonMapper.builder()
          .addModule(kotlinModule())
          .addModule(JavaTimeModule())
          .addModule(jacksonModule())
          .build()

  fun jacksonModule(): SimpleModule =
      SimpleModule("jooq-json").apply {
        addSerializer(JSON::class.java, JooqJsonSerializer())
        addSerializer(JSONB::class.java, JooqJsonSerializer())
        addDeserializer(JSON::class.java, JsonDeserializer(JSON::json))
        addDeserializer(JSONB::class.java, JsonDeserializer(JSONB::jsonb))
      }
}

class JooqJsonConverterProvider(
    private val mapper: ObjectMapper,
) : ConverterProvider {
  private val delegate = DefaultConverterProvider()

  @Suppress("UNCHECKED_CAST")
  override fun <T : Any, U : Any> provide(tType: Class<T>, uType: Class<U>): Converter<T, U> {
    val converter =
        when (tType) {
          JSON::class.java if uType == JsonNode::class.java -> JsonToNodeConverter(mapper)
          JSONB::class.java if uType == JsonNode::class.java -> JsonbToNodeConverter(mapper)
          else -> delegate.provide(tType, uType)
        }
    return converter as? Converter<T, U>
        ?: throw IllegalArgumentException(
            "No jOOQ converter available from ${tType.name} to ${uType.name}",
        )
  }
}

private class JsonToNodeConverter(
    private val mapper: ObjectMapper,
) : AbstractConverter<JSON, JsonNode>(JSON::class.java, JsonNode::class.java) {
  override fun from(databaseObject: JSON?): JsonNode? = databaseObject?.let {
    mapper.readTree(it.data())
  }

  override fun to(userObject: JsonNode?): JSON? = userObject?.let {
    JSON.json(mapper.writeValueAsString(it))
  }
}

private class JsonbToNodeConverter(
    private val mapper: ObjectMapper,
) : AbstractConverter<JSONB, JsonNode>(JSONB::class.java, JsonNode::class.java) {
  override fun from(databaseObject: JSONB?): JsonNode? = databaseObject?.let {
    mapper.readTree(it.data())
  }

  override fun to(userObject: JsonNode?): JSONB? = userObject?.let {
    JSONB.jsonb(mapper.writeValueAsString(it))
  }
}

private class JooqJsonSerializer : JsonSerializer<Any>() {
  @Throws(IOException::class)
  override fun serialize(value: Any, generator: JsonGenerator, provider: SerializerProvider) {
    val rawJson =
        when (value) {
          is JSON -> value.data()
          is JSONB -> value.data()
          else -> throw DataTypeException("Unsupported jOOQ JSON type: ${value.javaClass.name}")
        }
    generator.writeRawValue(rawJson)
  }
}

private class JsonDeserializer<T>(
    private val wrap: (String) -> T,
) : StdDeserializer<T>(Any::class.java) {
  @Throws(IOException::class)
  override fun deserialize(parser: JsonParser, context: DeserializationContext): T =
      wrap(parser.codec.readTree<JsonNode>(parser).toString())
}
