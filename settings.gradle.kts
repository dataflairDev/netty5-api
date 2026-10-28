plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "netty5-api"
include("netty5-connection")
include("netty5-demo")
