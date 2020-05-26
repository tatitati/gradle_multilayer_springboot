rootProject.name = "root"

include("myapp", "domain", "infrastructure", "ui")

project(":myapp").projectDir = file("myapp")
project(":domain").projectDir = file("subprojects/domain")
project(":infrastructure").projectDir = file("subprojects/infrastructure")
project(":ui").projectDir = file("subprojects/ui")


