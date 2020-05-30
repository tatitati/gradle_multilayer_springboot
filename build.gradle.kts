import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.springframework.boot.gradle.tasks.bundling.BootJar

description = "My project description here"

plugins {
	id("org.springframework.boot") version "2.3.0.RELEASE" apply false
	id("io.spring.dependency-management") version "1.0.9.RELEASE" apply false
	id("org.jetbrains.kotlin.jvm") version "1.3.72"
	id("org.jetbrains.kotlin.plugin.spring") version "1.3.72" apply false
}

allprojects {
	apply(plugin = "org.springframework.boot")
	apply(plugin = "io.spring.dependency-management")
	apply(plugin = "org.jetbrains.kotlin.jvm")
	apply(plugin = "org.jetbrains.kotlin.plugin.spring")

	group = "com.example"
	version = "0.0.1-SNAPSHOT"

	java {
		sourceCompatibility = JavaVersion.VERSION_1_8
		targetCompatibility = JavaVersion.VERSION_1_8
	}

	repositories {
		mavenCentral()
	}

	dependencies {
		implementation("org.springframework.boot:spring-boot-starter-web")
		implementation("org.springframework.boot:spring-boot-starter")
		implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
		implementation("org.jetbrains.kotlin:kotlin-reflect")
		implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
		implementation("org.apache.kafka:kafka-clients:2.4.0")
		implementation("org.apache.kafka:kafka-streams:2.4.0")
		implementation("org.apache.kafka:kafka-streams-test-utils:2.4.0")
		implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
		testImplementation("org.springframework.boot:spring-boot-starter-test") {
			exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
		}
	}

	tasks {
		withType<Test>() {
			useJUnitPlatform()
		}

		withType<KotlinCompile> {
			kotlinOptions {
				freeCompilerArgs = listOf("-Xjsr305=strict")
				jvmTarget = "1.8"
				suppressWarnings = true
			}
		}
	}
}

subprojects {
	tasks {
		withType<Jar>() {
			enabled = true
		}

		withType<BootJar>() {
			enabled = false
		}
	}
}

project(":domain"){
	description = "my domain layer description here"
}

project(":infrastructure"){
	description = "my infrastructure layer description here"

	dependencies{
		implementation(project(":domain"))
	}
}

project(":application"){
	description = "my APPLICATION layer description here"

	dependencies{
		implementation(project(":infrastructure"))
		implementation(project(":domain"))
	}
}

project(":ui"){
	description = "my UI layer description here"

	dependencies{
		implementation(project(":application"))
		implementation(project(":domain"))
		implementation(project(":infrastructure"))
	}
}

dependencies{
	implementation(project(":ui"))
}

