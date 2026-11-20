package com.theok.kra.platform.database

import io.r2dbc.spi.ConnectionFactory
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import org.jooq.ConverterProvider
import org.jooq.DSLContext
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.springframework.r2dbc.connection.ConnectionFactoryUtils

/**
 * Runs jOOQ against Spring's current R2DBC transaction connection.
 *
 * jOOQ must consume its publisher inside [execute]. Supplying the connection directly prevents jOOQ
 * from opening a second connection outside a declarative @Transactional boundary.
 */
class TransactionalJooqExecutor(
    private val connectionFactory: ConnectionFactory,
    private val converterProvider: ConverterProvider,
) {
  suspend fun <T> execute(block: suspend (DSLContext) -> T): T {
    val connection = ConnectionFactoryUtils.getConnection(connectionFactory).awaitSingle()
    try {
      val dsl = DSL.using(connection, SQLDialect.POSTGRES)
      dsl.configuration().set(converterProvider)
      return block(dsl)
    } finally {
      val transactionBound =
          ConnectionFactoryUtils.currentConnectionFactory(connectionFactory)
              .hasElement()
              .onErrorReturn(false)
              .awaitSingle()
      if (!transactionBound) {
        ConnectionFactoryUtils.releaseConnection(connection, connectionFactory).awaitFirstOrNull()
      }
    }
  }
}
