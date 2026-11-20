import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.compile.JavaCompile
import org.jooq.meta.jaxb.ForcedType

plugins {
  `java-library`
  alias(libs.plugins.kotlin.jvm)
  alias(libs.plugins.kotlin.spring)
  alias(libs.plugins.jooq.codegen)
}

kotlin { jvmToolchain(25) }

@Suppress("UNCHECKED_CAST")
val dotenv = rootProject.extensions.extraProperties["dotenv"] as Map<String, String>

fun localEnvironment(name: String): String? = dotenv[name] ?: System.getenv(name)

val databaseJdbcUrl = localEnvironment("KRA_DB_JDBC_URL")
val databaseUser = localEnvironment("KRA_DB_MIGRATOR_USER")
val databasePassword = localEnvironment("KRA_DB_MIGRATOR_PASSWORD")
val changelogResourcesDirectory = layout.projectDirectory.dir("src/main/resources")
val trackedGeneratedJooqDirectory = layout.projectDirectory.dir("src/generated/jooq")
val liquibaseCli = configurations.create("liquibaseCli")

dependencies {
  api(project(":voucher-domain"))
  implementation(project(":platform-database"))
  implementation("org.springframework.boot:spring-boot-starter-webflux")
  implementation("org.springframework.boot:spring-boot-starter-validation")
  // Spring Boot 4's WebFlux codecs use Jackson 3. This enables Kotlin data-class request bodies.
  implementation("tools.jackson.module:jackson-module-kotlin")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
  implementation("org.jooq:jooq-jackson-extensions:${libs.versions.jooq.get()}")

  jooqCodegen("org.postgresql:postgresql:42.7.11")
  jooqCodegen("org.jooq:jooq-jackson-extensions:${libs.versions.jooq.get()}")
  liquibaseCli(
      platform(
          "org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}",
      ),
  )
  liquibaseCli("org.liquibase:liquibase-core")
  liquibaseCli("org.postgresql:postgresql:42.7.11")
}

jooq {
  configuration {
    logging = org.jooq.meta.jaxb.Logging.WARN
    jdbc {
      driver = "org.postgresql.Driver"
      url = databaseJdbcUrl ?: "jdbc:postgresql://localhost:5432/kra"
      user = databaseUser ?: "kra_migrator"
      password = databasePassword ?: ""
    }
    generator {
      database {
        name = "org.jooq.meta.postgres.PostgresDatabase"
        inputSchema = "kra"
        includes = "voucher|voucher_claim|voucher_outbox"
        excludes = "databasechangelog|databasechangeloglock|debezium_offset_storage"
        forcedTypes.add(
            ForcedType()
                .withUserType("com.fasterxml.jackson.databind.JsonNode")
                .withConverter("org.jooq.jackson.extensions.converters.JSONBtoJacksonConverter")
                .withIncludeTypes("(?i:jsonb)"),
        )
      }
      generate {
        isDaos = false
        isPojos = true
        isImmutablePojos = true
        isRecords = true
        isIndexes = true
        isRelations = true
        isImplicitJoinPathsToOne = false
        isImplicitJoinPathsToMany = false
        isImplicitJoinPathsManyToMany = false
      }
      target {
        packageName = "com.theok.kra.shell.voucher.generated.jooq"
        directory = trackedGeneratedJooqDirectory.asFile.path
        isClean = true
      }
    }
  }
}

// jOOQ generates into a tracked source directory. Normal compilation uses that snapshot and
// never invokes generation or requires a database.
sourceSets.main {
  java.setSrcDirs(listOf("src/main/java", trackedGeneratedJooqDirectory))
}

tasks.register<JavaExec>("dbMigrate") {
  group = "database"
  description =
      "Applies Voucher adapter Liquibase migrations to the configured local PostgreSQL database."
  classpath = liquibaseCli
  mainClass.set("liquibase.integration.commandline.Main")
  args(
      "--classpath=${changelogResourcesDirectory.asFile.absolutePath}",
      "--changeLogFile=db/changelog/db.changelog-master.yaml",
      "--url=${databaseJdbcUrl ?: "jdbc:postgresql://localhost:5432/kra"}",
      "--username=${databaseUser ?: "kra_migrator"}",
      "--password=${databasePassword ?: ""}",
      "--defaultSchemaName=kra",
      "update",
  )
}

val compileGeneratedJooq =
    tasks.register<JavaCompile>("compileGeneratedJooq") {
      group = "database"
      description = "Compiles Voucher-generated jOOQ types against the adapter classpath."
      dependsOn("jooqCodegen")
      source(trackedGeneratedJooqDirectory)
      classpath = sourceSets.main.get().compileClasspath
      destinationDirectory.set(layout.buildDirectory.dir("classes/generated-jooq"))
      options.release.set(25)
    }

tasks.named("jooqCodegen") {
  mustRunAfter("dbMigrate")
}

tasks.withType<JavaCompile>().configureEach {
  mustRunAfter("jooqCodegen")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
  mustRunAfter("jooqCodegen")
}

tasks.register("verifyJooqCodegen") {
  group = "verification"
  description =
      "Migrates, generates, and compiles Voucher jOOQ types against the local PostgreSQL schema."
  dependsOn("dbMigrate", compileGeneratedJooq)
}

tasks.register("dbGenerateJooq") {
  group = "database"
  description = "Migrates, generates, and compiles jOOQ types owned by the Voucher adapter."
  dependsOn("verifyJooqCodegen")
}
