plugins {
  `java-library`
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.spring)
}

kotlin { jvmToolchain(25) }

dependencies {
  api(
      platform(
          "org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}",
      ),
  )
  api("org.springframework.boot:spring-boot-starter-data-r2dbc")
  api("org.jooq:jooq:${libs.versions.jooq.get()}")
  api("com.fasterxml.jackson.module:jackson-module-kotlin")
  api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")

  testImplementation(kotlin("test"))

  runtimeOnly("org.postgresql:r2dbc-postgresql")
  runtimeOnly("io.r2dbc:r2dbc-pool")
  runtimeOnly("org.postgresql:postgresql")
}

// Compatibility entry point required by the project contract. The Voucher adapter owns the
// migration resources and generated table model; platform-database owns only reusable database
// infrastructure.
tasks.register("dbGenerateJooq") {
  group = "database"
  description = "Verifies jOOQ types owned by feature adapters."
  dependsOn(":voucher-adapter:dbGenerateJooq", ":payment-adapter:dbGenerateJooq")
}
