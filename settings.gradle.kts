plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "netty-api"
include("netty-connection")
include("netty-demo")
