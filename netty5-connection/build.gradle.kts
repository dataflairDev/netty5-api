plugins {
    id("java")
}

java {
    withSourcesJar()
}

dependencies {
    compileOnly(libs.utility.lombok)
    compileOnly(libs.jetbrains.annotations)
    annotationProcessor(libs.utility.lombok)

    // Netty types (Channel, EventLoopGroup, ByteBuf) are part of the public API.
    api(platform(libs.netty.bom))
    api(libs.bundles.netty)
    runtimeOnly(variantOf(libs.netty.transport.native.epoll) { classifier("linux-x86_64") })
    runtimeOnly(variantOf(libs.netty.transport.native.epoll) { classifier("linux-aarch_64") })

    // CodecBuffer exposes JsonObject and Netty5ChannelUtils loads Gson statically.
    api(libs.gson)

    testCompileOnly(libs.jetbrains.annotations)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test {
    useJUnitPlatform()
    // every test class gets a fresh JVM so the leak test can install its detector before Netty initialises
    forkEvery = 1
    systemProperty("io.netty.leakDetection.level", "paranoid")
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

publishing {
    repositories {
        maven {
            name = "dataflair-public"
            url = uri(
                if (version.toString()
                        .endsWith("SNAPSHOT")
                ) "https://rp1.dataflair.cloud/repository/dataflair-public-development/"
                else "https://rp1.dataflair.cloud/repository/dataflair-public-release/"
            )
            credentials {
                username = project.findProperty("dataflair_rp1_admin_username") as String?
                password = project.findProperty("dataflair_rp1_admin_password") as String?
            }
        }
    }

    publications {
        create<MavenPublication>("dataflair-public") {
            from(components["java"])
        }
    }
}
