plugins {
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.spring)
  alias(libs.plugins.spring.boot)
}

kotlin { jvmToolchain(25) }

dependencies {
  implementation(project(":voucher-adapter"))
  implementation(project(":payment-adapter"))
  implementation(project(":platform-database"))
  implementation(project(":platform-web"))
  implementation("org.springframework.boot:spring-boot-starter")
  implementation("org.springframework.boot:spring-boot-starter-actuator")
  implementation("org.liquibase:liquibase-core")
  runtimeOnly("org.postgresql:postgresql")

  testImplementation("org.springframework.boot:spring-boot-starter-test")
  testImplementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
  testImplementation(libs.archunit.junit5)
  testImplementation(libs.testcontainers.postgres)
  testImplementation(libs.stripe.java)
  testImplementation(libs.temporal.sdk)
  testImplementation("com.rabbitmq:amqp-client")
  testImplementation("org.springframework.boot:spring-boot-starter-amqp")
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
  testImplementation(kotlin("test"))
}
