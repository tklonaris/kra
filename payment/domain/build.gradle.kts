plugins {
  alias(libs.plugins.kotlin.jvm)
}

kotlin { jvmToolchain(25) }

dependencies {
  api(project(":foundation-domain"))
  testImplementation(kotlin("test"))
}
