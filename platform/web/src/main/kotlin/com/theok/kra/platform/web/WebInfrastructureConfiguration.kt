package com.theok.kra.platform.web

import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import

/** Explicit application-composition entry point for reusable WebFlux infrastructure. */
@Configuration(proxyBeanMethods = false)
@Import(ApiExceptionHandler::class)
class WebInfrastructureConfiguration
