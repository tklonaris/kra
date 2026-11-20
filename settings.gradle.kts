pluginManagement {
  repositories {
    gradlePluginPortal()
    mavenCentral()
  }
}

dependencyResolutionManagement {
  repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
  repositories {
    mavenCentral()
  }
}

rootProject.name = "kotlin-reference-architecture"

include(
    ":foundation-domain",
    ":voucher-domain",
    ":voucher-adapter",
    ":payment-domain",
    ":payment-adapter",
    ":platform-database",
    ":platform-web",
    ":app",
)

project(":foundation-domain").projectDir = file("foundation/domain")

project(":voucher-domain").projectDir = file("voucher/domain")

project(":voucher-adapter").projectDir = file("voucher/adapter")

project(":payment-domain").projectDir = file("payment/domain")

project(":payment-adapter").projectDir = file("payment/adapter")

project(":platform-database").projectDir = file("platform/database")

project(":platform-web").projectDir = file("platform/web")
