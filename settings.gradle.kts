rootProject.name = "myapp"

include("domain", "infrastructure", "ui")

project(":domain").projectDir = file("subprojects/domain")
project(":infrastructure").projectDir = file("subprojects/infrastructure")
project(":ui").projectDir = file("subprojects/ui")


