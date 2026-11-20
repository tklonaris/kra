package com.theok.kra.platform.database

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.theok.kra.platform.database.json.JooqJsonConverterProvider
import com.theok.kra.platform.database.json.JooqJsonSupport
import io.r2dbc.spi.ConnectionFactory
import org.jooq.ConverterProvider
import org.springframework.beans.factory.ObjectProvider
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.ReactiveTransactionManager

@Configuration(proxyBeanMethods = false)
class DatabaseInfrastructureConfiguration {
  @Bean
  fun transactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager =
      R2dbcTransactionManager(connectionFactory)

  @Bean
  fun transactionalJooqExecutor(
      connectionFactory: ConnectionFactory,
      jooqJsonConverterProvider: ConverterProvider,
  ) = TransactionalJooqExecutor(connectionFactory, jooqJsonConverterProvider)

  @Bean fun objectMapper(): ObjectMapper = JooqJsonSupport.objectMapper()

  @Bean
  fun jooqJsonConverterProvider(objectMapper: ObjectProvider<ObjectMapper>): ConverterProvider =
      JooqJsonConverterProvider(objectMapper.getIfAvailable() ?: JooqJsonSupport.objectMapper())

  @Bean fun jooqJacksonModule(): SimpleModule = JooqJsonSupport.jacksonModule()
}
