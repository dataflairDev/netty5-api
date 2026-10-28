plugins {
    id("java")
    application
}

dependencies {
    compileOnly(libs.jetbrains.annotations)
    implementation(project(":netty5-connection"))

    testCompileOnly(libs.jetbrains.annotations)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}

application {
    mainClass.set("de.dataflair.netty5.demo.DemoApplication")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
