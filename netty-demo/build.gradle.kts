plugins {
    id("java")
    application
}

dependencies {
    compileOnly(libs.jetbrains.annotations)
    implementation(project(":netty-connection"))
    runtimeOnly(libs.slf4j.simple)

    testCompileOnly(libs.jetbrains.annotations)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("de.dataflair.netty.demo.DemoApplication")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
