package com.theok.kra.app

import com.theok.kra.platform.database.DatabaseInfrastructureConfiguration
import com.theok.kra.platform.web.WebInfrastructureConfiguration
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Import

@SpringBootApplication(
    scanBasePackages = ["com.theok.kra.shell"],
    excludeName =
        [
            "org.springframework.boot.security.autoconfigure.actuate.web.reactive.ReactiveManagementWebSecurityAutoConfiguration",
            "org.springframework.boot.security.autoconfigure.web.reactive.ReactiveWebSecurityAutoConfiguration",
        ],
)
@Import(
    DatabaseInfrastructureConfiguration::class,
    WebInfrastructureConfiguration::class,
    LiquibaseMigrationConfiguration::class,
)
class KotlinReferenceArchitectureApplication

fun main(args: Array<String>) {
  runApplication<KotlinReferenceArchitectureApplication>(*args)
}
