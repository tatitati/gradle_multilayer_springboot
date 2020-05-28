rootProject.name = "myapp"

include("domain", "infrastructure", "ui", "application")

project(":domain").projectDir = file("subprojects/domain")
project(":infrastructure").projectDir = file("subprojects/infrastructure")
project(":ui").projectDir = file("subprojects/ui")
project(":application").projectDir = file("subprojects/application")


