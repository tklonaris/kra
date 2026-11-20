package com.theok.kra.app.database

import io.r2dbc.spi.ValidationDepth
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.boot.r2dbc.autoconfigure.R2dbcProperties
import org.springframework.boot.r2dbc.autoconfigure.R2dbcProperties.Pool
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.io.ClassPathResource

class R2dbcPoolPropertiesTest {
  @Test
  fun `pool safeguards bind to Spring Boot R2DBC properties`() {
    val environment =
        StandardEnvironment().apply {
          propertySources.addFirst(
              YamlPropertySourceLoader()
                  .load("application", ClassPathResource("application.yaml"))
                  .single(),
          )
          propertySources.addFirst(
              MapPropertySource(
                  "pool-environment-overrides",
                  mapOf(
                      "KRA_R2DBC_POOL_MAX_SIZE" to "7",
                  ),
              ),
          )
        }
    val resolvedPoolProperties =
        ConfigurationPropertySources.from(
                MapPropertySource(
                    "resolved-pool-properties",
                    poolPropertyNames.associateWith(environment::getRequiredProperty),
                ),
            )
            .filterNotNull()
    val properties =
        Binder(resolvedPoolProperties)
            .bind("spring.r2dbc", Bindable.of(R2dbcProperties::class.java))
            .orElseThrow { IllegalStateException("R2DBC properties did not bind") }
    val pool: Pool = properties.pool

    assertEquals(5, pool.initialSize)
    assertEquals(0, pool.minIdle)
    assertEquals(7, pool.maxSize)
    assertEquals(Duration.ofSeconds(20), pool.maxAcquireTime)
    assertEquals(2, pool.acquireRetry)
    assertEquals(Duration.ofSeconds(5), pool.maxCreateConnectionTime)
    assertEquals(Duration.ofSeconds(10), pool.maxValidationTime)
    assertEquals(Duration.ofMinutes(3), pool.maxIdleTime)
    assertEquals(Duration.ofMinutes(4), pool.maxLifeTime)
    assertEquals(ValidationDepth.REMOTE, pool.validationDepth)
    assertEquals("SELECT 1", pool.validationQuery)
  }

  private companion object {
    val poolPropertyNames =
        listOf(
            "spring.r2dbc.pool.enabled",
            "spring.r2dbc.pool.initial-size",
            "spring.r2dbc.pool.min-idle",
            "spring.r2dbc.pool.max-size",
            "spring.r2dbc.pool.max-acquire-time",
            "spring.r2dbc.pool.acquire-retry",
            "spring.r2dbc.pool.max-create-connection-time",
            "spring.r2dbc.pool.max-validation-time",
            "spring.r2dbc.pool.max-idle-time",
            "spring.r2dbc.pool.max-life-time",
            "spring.r2dbc.pool.validation-depth",
            "spring.r2dbc.pool.validation-query",
        )
  }
}
