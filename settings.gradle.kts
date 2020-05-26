rootProject.name = "myapp"

include("domain", "infrastructure")

project(":domain").projectDir = file("subprojects/domain")
project(":infrastructure").projectDir = file("subprojects/infrastructure")


