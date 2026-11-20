package com.theok.kra.app.architecture

import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices
import kotlin.test.Test

class ArchitectureTest {
  private val classes = ClassFileImporter().importPackages("com.theok.kra")

  @Test
  fun `functional core does not depend on infrastructure or frameworks`() {
    noClasses()
        .that()
        .resideInAnyPackage(
            "com.theok.kra.core..",
        )
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "org.springframework..",
            "org.jooq..",
            "io.r2dbc..",
            "io.temporal..",
            "com.rabbitmq..",
            "com.stripe..",
            "com.fasterxml.jackson..",
            "tools.jackson..",
            "reactor..",
            "kotlinx.coroutines..",
        )
        .check(classes)
  }

  @Test
  fun `domain models do not depend on DTOs`() {
    noClasses()
        .that()
        .resideInAPackage("com.theok.kra.core..domain..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("com.theok.kra.core..dto..")
        .check(classes)
  }

  @Test
  fun `functional core does not depend on the imperative architecture`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.core..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.platform..",
            "com.theok.kra.shell..",
            "com.theok.kra.app..",
        )
        .check(classes)
  }

  @Test
  fun `shared platform does not depend on application or feature code`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.platform..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.core..",
            "com.theok.kra.shell..",
            "com.theok.kra.app..",
        )
        .check(classes)
  }

  @Test
  fun `feature adapters do not depend on each other`() {
    slices().matching("com.theok.kra.shell.(*)..").should().notDependOnEachOther().check(classes)
  }

  @Test
  fun `feature adapters do not depend on the application`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("com.theok.kra.app..")
        .check(classes)
  }

  @Test
  fun `project does not use DatabaseClient`() {
    noClasses()
        .should()
        .dependOnClassesThat()
        .haveFullyQualifiedName("org.springframework.r2dbc.core.DatabaseClient")
        .check(classes)
  }

  @Test
  fun `controllers do not depend on persistence or transaction infrastructure`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..controller..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..persistence..",
            "com.theok.kra.platform.database..",
            "com.theok.kra.shell..generated.jooq..",
            "org.jooq..",
            "org.springframework.data.r2dbc..",
            "org.springframework.r2dbc..",
            "org.springframework.transaction..",
            "io.r2dbc..",
        )
        .check(classes)
  }

  @Test
  fun `persistence does not depend on controllers`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..persistence..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage("com.theok.kra.shell..controller..")
        .check(classes)
  }

  @Test
  fun `persistence does not depend on higher feature layers`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..persistence..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..commands..",
            "com.theok.kra.shell..queries..",
            "com.theok.kra.shell..workflow..",
            "com.theok.kra.shell..messaging..",
        )
        .check(classes)
  }

  @Test
  fun `controllers do not bypass commands and queries through feature orchestration`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..controller..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..configuration..",
            "com.theok.kra.shell..crypto..",
            "com.theok.kra.shell..workflow..",
            "com.theok.kra.shell..messaging..",
        )
        .check(classes)
  }

  @Test
  fun `queries do not depend on commands or persistence`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..queries..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..commands..",
            "com.theok.kra.shell..persistence..",
        )
        .check(classes)
  }

  @Test
  fun `commands do not depend on read or asynchronous orchestration`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..commands..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..controller..",
            "com.theok.kra.shell..queries..",
            "com.theok.kra.shell..workflow..",
            "com.theok.kra.shell..messaging..",
        )
        .check(classes)
  }

  @Test
  fun `queries do not depend on asynchronous orchestration`() {
    noClasses()
        .that()
        .resideInAnyPackage("com.theok.kra.shell..queries..")
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "com.theok.kra.shell..controller..",
            "com.theok.kra.shell..workflow..",
            "com.theok.kra.shell..messaging..",
        )
        .check(classes)
  }

  @Test
  fun `feature orchestration does not depend on jOOQ or generated tables`() {
    noClasses()
        .that()
        .resideInAnyPackage(
            "..shell..commands..",
            "..shell..controller..",
            "..shell..messaging..",
            "..shell..workflow..",
        )
        .should()
        .dependOnClassesThat()
        .resideInAnyPackage(
            "org.jooq..",
            "..generated.jooq..",
        )
        .check(classes)
  }
}
