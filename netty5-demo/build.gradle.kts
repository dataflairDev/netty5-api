plugins {
    id("java")
}

dependencies {
    compileOnly(libs.utility.lombok)
    compileOnly(libs.jetbrains.annotations)
    annotationProcessor(libs.utility.lombok)
    implementation(project(":netty5-connection"))
}
