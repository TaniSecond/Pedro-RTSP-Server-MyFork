plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.jetbrains.kotlin)
}

android {
    namespace = "com.pedro.sample"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.pedro.sample"
        minSdk = 23
        targetSdk = 36
        // バージョンに "-sub" 等のサフィックスが付いても動くよう数字のみ抽出する。
        versionCode = project.version.toString().filter { it.isDigit() }.toInt()
        versionName = project.version.toString()
    }
    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlin {
        jvmToolchain(17)
    }
}

dependencies {
    implementation(project(":rtspserver"))
    implementation(libs.rootEncoder.library)
    implementation(libs.rootEncoder.extra.sources)
    implementation(libs.androidx.appcompat)
    implementation(libs.androidx.constraintlayout)
}
