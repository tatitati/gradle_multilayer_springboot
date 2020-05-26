plugins {
    id("org.springframework.boot")
    id("io.spring.dependency-management")
    id("org.jetbrains.kotlin.jvm")
    id("org.jetbrains.kotlin.plugin.spring")
    application
}


dependencies{
    implementation(project(":ui"))
    implementation(project(":domain"))
    implementation(project(":infrastructure"))
}

application {
    mainClassName = "App.Main"
}
