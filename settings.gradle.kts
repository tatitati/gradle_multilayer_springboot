rootProject.name = "myapp"

include("domain", "infrastructure", "presentation", "application")

project(":domain").projectDir = file("subprojects/domain")
project(":infrastructure").projectDir = file("subprojects/infrastructure")
project(":presentation").projectDir = file("subprojects/presentation")
project(":application").projectDir = file("subprojects/application")
