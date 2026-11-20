package com.theok.kra.app

import java.sql.DriverManager
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment

/** Applies application-owned schema changes before the HTTP server and feature workers start. */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
    prefix = "spring.liquibase",
    name = ["enabled"],
    havingValue = "true",
    matchIfMissing = true,
)
class LiquibaseMigrationConfiguration {
  @Bean
  fun applicationSchemaMigrator(environment: Environment): InitializingBean = InitializingBean {
    DriverManager.getConnection(
            environment.getRequiredProperty("spring.liquibase.url"),
            environment.getRequiredProperty("spring.liquibase.user"),
            environment.getRequiredProperty("spring.liquibase.password"),
        )
        .use { connection ->
          val database =
              DatabaseFactory.getInstance()
                  .findCorrectDatabaseImplementation(JdbcConnection(connection))
          database.defaultSchemaName =
              environment.getProperty("spring.liquibase.default-schema", "kra")
          Liquibase(
                  "db/changelog/app-master.yaml",
                  ClassLoaderResourceAccessor(),
                  database,
              )
              .update(Contexts(), LabelExpression())
        }
  }
}
