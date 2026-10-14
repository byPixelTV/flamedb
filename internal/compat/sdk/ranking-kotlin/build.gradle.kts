plugins {
    kotlin("jvm") version "2.4.20"
    kotlin("plugin.serialization") version "2.4.20"
}
repositories { mavenCentral() }
dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
}
kotlin {
    jvmToolchain(21)
    sourceSets.main {
        kotlin.srcDir(file("../../../../pkg/kotlin/src/main/kotlin"))
        kotlin.srcDir(file("src"))
    }
}
tasks.register<JavaExec>("runRanking") {
    dependsOn("classes")
    classpath = sourceSets.main.get().runtimeClasspath
    mainClass.set("consumer.RankingKt")
    args(providers.gradleProperty("sdkAddr").get(), providers.gradleProperty("sdkPrefix").get())
}
