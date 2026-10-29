import com.vanniktech.maven.publish.JavaLibrary
import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.SourcesJar
import org.gradle.plugins.signing.SigningExtension

plugins {
    id("java")
    alias(libs.plugins.maven.publish)
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

    // CodecBuffer exposes JsonObject and NettyChannelUtils loads Gson statically.
    api(libs.gson)

    // Logging facade; applications choose the binding.
    api(libs.slf4j.api)

    testCompileOnly(libs.jetbrains.annotations)
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testRuntimeOnly(libs.slf4j.simple)
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

// Published to Maven Central through the Central Portal. Coordinates and the remaining POM fields come from
// gradle.properties (GROUP, VERSION_NAME, POM_*); credentials and signing keys only from ~/.gradle/gradle.properties
// or ORG_GRADLE_PROJECT_* environment variables. See README, "Releasing".
mavenPublishing {
    configure(JavaLibrary(javadocJar = JavadocJar.Javadoc(), sourcesJar = SourcesJar.Sources()))
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()

    pom {
        licenses {
            license {
                name.set("Apache-2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
    }
}

// The shadow plugin (applied to all projects) adds the fat "-all" jar to the java component. A library on Maven
// Central must not ship Netty, Gson and SLF4J inside its own jar, so that variant is not published.
(components["java"] as AdhocComponentWithVariants)
    .withVariantsFromConfiguration(configurations["shadowRuntimeElements"]) { skip() }

// Signing is mandatory for every remote publication and for explicitly requested sign tasks. For
// publishToMavenLocal it is optional: without a configured key the signature files are simply not produced.
val signingRequired = gradle.startParameter.taskNames
    .map { it.substringAfterLast(':') }
    .any { (it.startsWith("publish") && !it.endsWith("ToMavenLocal")) || it.startsWith("sign") }
plugins.withId("signing") {
    extensions.configure<SigningExtension> {
        isRequired = signingRequired
    }
}

tasks.withType<Javadoc>().configureEach {
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        // javadoc does not run Lombok, so links to Lombok accessors cannot resolve and missing comments
        // are not errors; broken HTML and syntax still fail the build
        addStringOption("Xdoclint:all,-missing,-reference", "-quiet")
    }
}

// Apache-2.0 section 4(d): the NOTICE travels with every distributed jar
tasks.withType<Jar>().configureEach {
    from(rootProject.files("LICENSE", "NOTICE")) {
        into("META-INF")
    }
}
