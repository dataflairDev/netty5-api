import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("java")
    alias(libs.plugins.shadow)
}

allprojects {
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "com.gradleup.shadow")

    group = "de.dataflair.netty5-api"
    version = "2.0.0"

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(27))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(27)
        options.encoding = "UTF-8"
    }

    tasks.withType<Jar>().configureEach {
        manifest {
            attributes(
                "Implementation-Version" to project.version
            )
        }
    }

    tasks.named<ShadowJar>("shadowJar") {
        mergeServiceFiles()
    }
}
