import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.jetbrains.kotlin.jvm") version "1.6.21"
}

object Versions {
    const val AWSSDK = "2.20.151"
    const val ICEBERG = "1.4.2"
    const val SPARK = "3.4.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.logging.log4j:log4j-api:2.20.0")
    implementation("org.apache.logging.log4j:log4j-core:2.20.0")
    // Use the Kotlin test library.
//    testImplementation("org.jetbrains.kotlin:kotlin-test")
//    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
//    api("org.apache.commons:commons-math3:3.6.1")
    implementation("org.apache.iceberg:iceberg-core:${Versions.ICEBERG}")
    implementation("org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:${Versions.ICEBERG}")
//    implementation("org.apache.iceberg:iceberg-spark3-extensions:0.13.2")
    implementation("org.apache.spark:spark-core_2.12:${Versions.SPARK}")
    implementation("org.apache.spark:spark-sql_2.12:${Versions.SPARK}")
    implementation("javax.servlet:javax.servlet-api:4.0.1")
    implementation("software.amazon.awssdk:glue:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:sts:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:athena:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:s3:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:kms:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:dynamodb:${Versions.AWSSDK}")
    implementation("software.amazon.awssdk:redshiftdata:${Versions.AWSSDK}")
    implementation("software.amazon.awscdk:msk:1.204.0")
    implementation("com.github.javafaker:javafaker:1.0.2")
}


tasks.apply {
    test {
        maxParallelForks = 1
        enableAssertions = true
        useJUnitPlatform {}
    }

    withType<KotlinCompile> {
        kotlinOptions {
            jvmTarget = "11"
            freeCompilerArgs = listOf("-Xjsr305=strict", "-Xinline-classes", "-Xcontext-receivers")
        }
    }
}
