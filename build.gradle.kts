import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    id("java")
    alias(libs.plugins.shadow)
    alias(libs.plugins.maven.publish) apply false
}

allprojects {
    apply(plugin = "java-library")
    apply(plugin = "com.gradleup.shadow")

    // GROUP and VERSION_NAME live in gradle.properties; VERSION_NAME can be overridden with -PVERSION_NAME=...
    group = providers.gradleProperty("GROUP").getOrElse("io.github.dataflairdev")
    version = providers.gradleProperty("VERSION_NAME").get()

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
