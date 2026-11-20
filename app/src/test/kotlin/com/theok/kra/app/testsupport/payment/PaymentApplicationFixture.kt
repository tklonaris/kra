package com.theok.kra.app.testsupport.payment

import com.fasterxml.jackson.databind.ObjectMapper
import com.theok.kra.app.KotlinReferenceArchitectureApplication
import com.theok.kra.app.testsupport.infrastructure.PostgresFixture
import com.theok.kra.core.payment.domain.PaymentOwnerId
import com.theok.kra.platform.database.TransactionalJooqExecutor
import com.theok.kra.shell.payment.commands.PaymentCommands
import com.theok.kra.shell.payment.commands.PaymentWebhookCommands
import com.theok.kra.shell.payment.configuration.PaymentApiAccessMode
import com.theok.kra.shell.payment.persistence.coordination.PaymentWorkflowPersister
import com.theok.kra.shell.payment.persistence.delivery.PaymentConsumerEventPersister
import java.util.Base64
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.web.server.context.WebServerApplicationContext
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.testcontainers.postgresql.PostgreSQLContainer
import reactor.core.publisher.Mono

/** Application and PostgreSQL setup shared by focused payment integration tests. */
internal class PaymentApplicationFixture(
    private val webApplicationType: WebApplicationType = WebApplicationType.NONE,
    private val paymentApiAccessModeOverride: PaymentApiAccessMode? = null,
    private val trustedDemoOwnerOverride: PaymentOwnerId? = PaymentOwnerId("test-payment-owner"),
    private val includePaymentApiAccessTestJwtDecoder: Boolean = true,
    private val additionalApplicationArguments: List<String> = emptyList(),
) : AutoCloseable {
  val paymentOwner = trustedDemoOwnerOverride ?: PaymentOwnerId("local-demo-owner")
  val postgres = PostgreSQLContainer("postgres:18.6")
  lateinit var commands: PaymentCommands
    private set

  lateinit var webhookCommands: PaymentWebhookCommands
    private set

  lateinit var eventPersister: PaymentConsumerEventPersister
    private set

  lateinit var jooq: TransactionalJooqExecutor
    private set

  lateinit var workflowTransactions: PaymentWorkflowPersister
    private set

  lateinit var database: PaymentDatabaseFixture
    private set

  lateinit var objectMapper: ObjectMapper
    private set

  lateinit var applicationUrl: String
    private set

  private lateinit var context: ConfigurableApplicationContext

  fun start() {
    postgres.start()
    PostgresFixture.bootstrap(postgres)
    PostgresFixture.migrate(postgres, "db/changelog/app-master.yaml")
    val applicationArguments =
        listOf(
            "--spring.r2dbc.url=${PostgresFixture.r2dbcUrl(postgres)}",
            "--spring.r2dbc.username=${PostgresFixture.APP_USER}",
            "--spring.r2dbc.password=${PostgresFixture.APP_PASSWORD}",
            "--spring.r2dbc.pool.initial-size=0",
            "--spring.liquibase.enabled=false",
            "--payments.stripe.secret-key=sk_test_unused",
            "--payments.stripe.webhook-secret=whsec_test_unused",
            "--payments.stripe.client-secret-encryption-key=${Base64.getEncoder().encodeToString(ByteArray(32))}",
            "--payments.worker.enabled=false",
        ) +
            paymentApiAccessArguments() +
            additionalApplicationArguments +
            if (webApplicationType == WebApplicationType.NONE) emptyList()
            else listOf("--server.port=0")
    val application = SpringApplicationBuilder(KotlinReferenceArchitectureApplication::class.java)
    if (includePaymentApiAccessTestJwtDecoder) {
      application.sources(PaymentApiAccessTestConfiguration::class.java)
    }
    context =
        application
            .profiles("payments", "test")
            .web(webApplicationType)
            .run(*applicationArguments.toTypedArray())
    commands = context.getBean(PaymentCommands::class.java)
    webhookCommands = context.getBean(PaymentWebhookCommands::class.java)
    eventPersister = context.getBean(PaymentConsumerEventPersister::class.java)
    jooq = context.getBean(TransactionalJooqExecutor::class.java)
    workflowTransactions = context.getBean(PaymentWorkflowPersister::class.java)
    database = PaymentDatabaseFixture(jooq, postgres)
    objectMapper = context.getBean(ObjectMapper::class.java)
    if (webApplicationType != WebApplicationType.NONE) {
      val webServer = requireNotNull((context as WebServerApplicationContext).webServer)
      applicationUrl = "http://localhost:${webServer.port}"
    }
  }

  override fun close() {
    if (::context.isInitialized) context.close()
    postgres.stop()
  }

  private fun paymentApiAccessArguments(): List<String> =
      when (paymentApiAccessModeOverride) {
        PaymentApiAccessMode.TRUSTED_DEMO ->
            listOf("--payments.api-access.mode=trusted-demo") +
                listOfNotNull(
                    trustedDemoOwnerOverride?.let {
                      "--payments.api-access.trusted-demo-owner-id=${it.value}"
                    },
                )
        PaymentApiAccessMode.OIDC -> listOf("--payments.api-access.mode=oidc")
        null -> emptyList()
      }
}

/** Supplies deterministic test JWTs without requiring an external OIDC provider. */
@TestConfiguration(proxyBeanMethods = false)
internal class PaymentApiAccessTestConfiguration {
  @Bean
  fun paymentApiAccessTestJwtDecoder(): ReactiveJwtDecoder = ReactiveJwtDecoder { token ->
    val jwt =
        Jwt.withTokenValue(token)
            .header("alg", "none")
            .issuer("https://issuer.test/kra")
            .subject(token)
    if (token == "payment-operator") jwt.claim("scope", "payments:retry")
    Mono.just(jwt.build())
  }
}
