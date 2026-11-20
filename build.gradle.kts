import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URI
import java.nio.file.Files
import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.TimeUnit

plugins {
  alias(libs.plugins.kotlin.jvm) apply false
  alias(libs.plugins.kotlin.spring) apply false
  alias(libs.plugins.spring.boot) apply false
  alias(libs.plugins.jooq.codegen) apply false
  alias(libs.plugins.spotless)
}

group = "io.github.theok"

version = "0.1.0-SNAPSHOT"

val dotenv =
    file(".env")
        .takeIf { it.isFile }
        ?.useLines { lines ->
          lines
              .map(String::trim)
              .filter { it.isNotEmpty() && !it.startsWith("#") }
              .mapNotNull { line ->
                val separator = line.indexOf('=')
                if (separator <= 0) null
                else line.substring(0, separator).trim() to line.substring(separator + 1)
              }
              .toMap()
        }
        .orEmpty()

extensions.extraProperties["dotenv"] = dotenv

fun manualHttpSmokeTask(name: String, description: String, composeFile: String, service: String) =
    tasks.register<Exec>(name) {
      group = "verification"
      this.description = description
      commandLine(
          "docker",
          "compose",
          "--env-file",
          ".env",
          "-f",
          composeFile,
          "run",
          "--rm",
          service,
      )
    }

manualHttpSmokeTask(
    "manualVoucherHttpSmoke",
    "Runs the manual voucher HTTP smoke test against an already running application.",
    "app/src/test/manual/voucher/compose.yaml",
    "voucher-manual-smoke",
)

manualHttpSmokeTask(
    "manualPaymentHttpSmoke",
    "Runs the manual Stripe HTTP smoke test against an already running payment application and Stripe CLI listener.",
    "app/src/test/manual/payment/compose.yaml",
    "payment-manual-smoke",
)

val manualVoucherSmoke =
    tasks.register("manualVoucherSmoke") {
      group = "verification"
      description =
          "Starts local infrastructure and the application before running the manual voucher smoke test."
      doLast {
        require(!applicationHealthy()) {
          "An application is already listening on localhost:8080. Stop it before running manualVoucherSmoke."
        }

        val logs = Files.createTempDirectory("kra-voucher-smoke-").toFile()
        val applicationOutput = logs.resolve("application.out.log")
        val applicationError = logs.resolve("application.err.log")
        var application: Process? = null
        var completed = false
        try {
          runManualSmokeCommand(
              projectDir,
              "docker",
              "compose",
              "--env-file",
              ".env",
              "-f",
              "infrastructure/compose.yaml",
              "up",
              "-d",
          )
          application =
              ProcessBuilder(
                      "cmd.exe",
                      "/c",
                      "gradlew.bat",
                      "--no-daemon",
                      ":app:bootRun",
                      "--args=--spring.profiles.active=voucher",
                  )
                  .directory(projectDir)
                  .redirectOutput(applicationOutput)
                  .redirectError(applicationError)
                  .start()
          awaitApplicationHealth(application, applicationOutput, applicationError)
          runManualSmokeCommand(
              projectDir,
              "docker",
              "compose",
              "--env-file",
              ".env",
              "-f",
              "app/src/test/manual/voucher/compose.yaml",
              "run",
              "--rm",
              "--no-TTY",
              "voucher-manual-smoke",
          )
          completed = true
        } finally {
          application?.stopProcessTree()
          if (completed) logs.deleteRecursively()
          else logger.lifecycle("Voucher smoke logs retained at ${logs.absolutePath}")
        }
      }
    }

val manualPaymentSmoke =
    tasks.register("manualPaymentSmoke") {
      group = "verification"
      description =
          "Starts local infrastructure, Stripe webhook forwarding, and the payment application before running the manual Stripe smoke test."
      doLast {
        val smokeDotenv = file(".env")
        require(smokeDotenv.isFile) {
          "Missing ${smokeDotenv.absolutePath}. Copy .env.example to .env and set STRIPE_API_KEY."
        }
        val stripeApiKey = dotenv["STRIPE_API_KEY"]
        require(!stripeApiKey.isNullOrBlank() && stripeApiKey.startsWith("sk_test_")) {
          "Set STRIPE_API_KEY in .env to a Stripe test-mode secret key."
        }
        requireStripeCli()
        require(!applicationHealthy()) {
          "An application is already listening on localhost:8080. Stop it before running manualPaymentSmoke."
        }

        val logs = Files.createTempDirectory("kra-payment-smoke-").toFile()
        var stripe: Process? = null
        var application: Process? = null
        var completed = false
        try {
          runManualSmokeCommand(
              projectDir,
              "docker",
              "compose",
              "--env-file",
              ".env",
              "-f",
              "infrastructure/compose.yaml",
              "up",
              "-d",
          )

          val stripeOutput = logs.resolve("stripe-listen.out.log")
          val stripeError = logs.resolve("stripe-listen.err.log")
          stripe =
              ProcessBuilder(
                      "cmd.exe",
                      "/c",
                      "stripe.cmd",
                      "listen",
                      "--events",
                      "payment_intent.succeeded,payment_intent.payment_failed,payment_intent.canceled",
                      "--forward-to",
                      "http://localhost:8080/payment-requests/stripe/webhook",
                  )
                  .directory(projectDir)
                  .redirectOutput(stripeOutput)
                  .redirectError(stripeError)
                  .also { process -> process.environment()["STRIPE_API_KEY"] = stripeApiKey }
                  .start()
          val webhookSecret = awaitWebhookSecret(stripe, stripeOutput, stripeError)

          val encryptionKey = dotenv["PAYMENTS_CLIENT_SECRET_ENCRYPTION_KEY"].validEncryptionKey()
          val applicationOutput = logs.resolve("application.out.log")
          val applicationError = logs.resolve("application.err.log")
          application =
              ProcessBuilder(
                      "cmd.exe",
                      "/c",
                      "gradlew.bat",
                      "--no-daemon",
                      ":app:bootRun",
                      "--args=--spring.profiles.active=payments,local --payments.api-access.mode=trusted-demo",
                  )
                  .directory(projectDir)
                  .redirectOutput(applicationOutput)
                  .redirectError(applicationError)
                  .also { process ->
                    process.environment()["STRIPE_API_KEY"] = stripeApiKey
                    process.environment()["STRIPE_WEBHOOK_SECRET"] = webhookSecret
                    process.environment()["PAYMENTS_CLIENT_SECRET_ENCRYPTION_KEY"] = encryptionKey
                  }
                  .start()
          awaitApplicationHealth(application, applicationOutput, applicationError)

          runManualSmokeCommand(
              projectDir,
              "docker",
              "compose",
              "--env-file",
              ".env",
              "-f",
              "app/src/test/manual/payment/compose.yaml",
              "run",
              "--rm",
              "--no-TTY",
              "payment-manual-smoke",
          )
          completed = true
        } finally {
          if (!providers.gradleProperty("keepManualPaymentSmokeRunning").isPresent) {
            application?.stopProcessTree()
            stripe?.stopProcessTree()
          }
          if (completed) logs.deleteRecursively()
          else logger.lifecycle("Payment smoke logs retained at ${logs.absolutePath}")
        }
      }
    }

manualPaymentSmoke.configure { mustRunAfter(manualVoucherSmoke) }

private fun requireStripeCli() {
  try {
    val process =
        ProcessBuilder("cmd.exe", "/c", "stripe.cmd", "--version").redirectErrorStream(true).start()
    check(process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0) {
      "Stripe CLI failed its version check. Install a working Stripe CLI, then retry manualPaymentSmoke."
    }
  } catch (failure: IOException) {
    error(
        "Stripe CLI is not on PATH. Install it before running manualPaymentSmoke.",
    )
  }
}

private fun applicationHealthy(): Boolean = runCatching {
  (URI("http://localhost:8080/actuator/health").toURL().openConnection() as HttpURLConnection).run {
    connectTimeout = 1_000
    readTimeout = 1_000
    requestMethod = "GET"
    responseCode == 200
  }
}
    .getOrDefault(false)

private fun awaitWebhookSecret(stripe: Process, output: File, errorLog: File): String {
  val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1)
  val secret = Regex("whsec_[A-Za-z0-9]+")
  while (System.nanoTime() < deadline) {
    listOf(output, errorLog)
        .filter(File::isFile)
        .asSequence()
        .map { it.readText() }
        .mapNotNull { secret.find(it)?.value }
        .firstOrNull()
        ?.let {
          return it
        }
    check(stripe.isAlive) {
      "Stripe CLI stopped before printing a webhook signing secret. Review ${output.absolutePath} and ${errorLog.absolutePath}."
    }
    Thread.sleep(500)
  }
  error(
      "Timed out waiting for Stripe CLI to print a webhook signing secret. Review ${output.absolutePath} and ${errorLog.absolutePath}.",
  )
}

private fun String?.validEncryptionKey(): String =
    takeUnless { it.isNullOrBlank() || it == "replace-me" }
        ?.let { value ->
          runCatching { Base64.getDecoder().decode(value) }
              .getOrNull()
              ?.takeIf { it.size == 32 }
              ?.let { value }
        } ?: ByteArray(32).also(SecureRandom()::nextBytes).let(Base64.getEncoder()::encodeToString)

private fun awaitApplicationHealth(application: Process, output: File, errorLog: File) {
  val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2)
  while (System.nanoTime() < deadline) {
    if (applicationHealthy()) return
    check(application.isAlive) {
      "The payment application stopped before becoming healthy. Review ${output.absolutePath} and ${errorLog.absolutePath}."
    }
    Thread.sleep(1_000)
  }
  error(
      "Timed out waiting for the payment application. Review ${output.absolutePath} and ${errorLog.absolutePath}.",
  )
}

private fun Process.stopProcessTree() {
  if (isAlive)
      ProcessBuilder("taskkill.exe", "/PID", pid().toString(), "/T", "/F").start().waitFor()
}

private fun runManualSmokeCommand(workingDirectory: File, vararg command: String) {
  val output = Files.createTempFile("kra-manual-command-", ".log").toFile()
  try {
    val process =
        ProcessBuilder(*command)
            .directory(workingDirectory)
            .redirectErrorStream(true)
            .redirectOutput(output)
            .start()
    check(process.waitFor() == 0) {
      "Command failed: ${command.joinToString(" ")}\n${output.readText()}"
    }
  } finally {
    output.delete()
  }
}

val structurizrContainerName = "kra-structurizr"
val structurizrImage = "structurizr/structurizr:latest"
val structurizrPort = providers.gradleProperty("structurizrPort").orElse("8081").get()
val structurizrWorkspace = layout.projectDirectory.dir("docs/architecture/structurizr").asFile

val structurizrValidate =
    tasks.register<Exec>("structurizrValidate") {
      group = "documentation"
      description = "Validates the Structurizr architecture workspace."
      commandLine(
          "docker",
          "run",
          "--rm",
          "-v",
          "${structurizrWorkspace.absolutePath}:/usr/local/structurizr",
          structurizrImage,
          "validate",
          "-workspace",
          "workspace.dsl",
      )
    }

val structurizrStop =
    tasks.register<Exec>("structurizrStop") {
      group = "documentation"
      description = "Stops the local Structurizr container if it is running."
      isIgnoreExitValue = true
      commandLine("docker", "rm", "-f", structurizrContainerName)
    }

tasks.register<Exec>("structurizr") {
  group = "documentation"
  description =
      "Validates and starts Structurizr Local at http://localhost:$structurizrPort (override with -PstructurizrPort)."
  dependsOn(structurizrValidate, structurizrStop)
  doFirst {
    require(structurizrPort.toIntOrNull() in 1..65535) {
      "structurizrPort must be an integer from 1 to 65535."
    }
  }
  commandLine(
      "docker",
      "run",
      "-d",
      "--rm",
      "--name",
      structurizrContainerName,
      "-p",
      "127.0.0.1:$structurizrPort:8080",
      "-v",
      "${structurizrWorkspace.absolutePath}:/usr/local/structurizr",
      structurizrImage,
      "local",
  )
}

structurizrStop.configure { mustRunAfter(structurizrValidate) }

spotless {
  kotlin {
    target("**/*.kt")
    targetExclude("**/build/**", ".gradle/**", "**/src/generated/**")
    ktfmt()
  }
  kotlinGradle {
    target("*.gradle.kts", "**/*.gradle.kts")
    ktfmt()
  }
}

// jOOQ writes tracked sources beneath the repository root. If generation and verification are
// requested together, order the root formatter after generation so Gradle does not report an
// implicit producer/consumer dependency.
tasks.named("spotlessKotlinGradle") {
  mustRunAfter(":payment-adapter:jooqCodegen", ":voucher-adapter:jooqCodegen")
}

subprojects {
  group = rootProject.group
  version = rootProject.version

  tasks.withType<JavaExec>().configureEach {
    environment(dotenv.filterKeys { System.getenv(it) == null })
  }

  tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    dependsOn(rootProject.tasks.named("spotlessCheck"))
  }

  tasks
      .matching { it.name == "check" }
      .configureEach {
        dependsOn(rootProject.tasks.named("spotlessCheck"))
      }
}
