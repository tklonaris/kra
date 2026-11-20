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
  api("org.springframework.boot:spring-boot-starter-webflux")

  testImplementation(kotlin("test"))
}
