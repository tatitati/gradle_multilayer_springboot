
## About the subprojects (layers) of this application
```
$ ./gradlew -q projects

------------------------------------------------------------
Root project - My project description here
------------------------------------------------------------

Root project 'myapp' - My project description here
+--- Project ':application' - my APPLICATION layer description here
+--- Project ':domain' - my domain layer description here
+--- Project ':infrastructure' - my infrastructure layer description here
\--- Project ':presentation' - my CLI/WEB layer description here
```

## How to run tests

```
./gradlew domain:test
./gradlew infrastructure:test
./gradlew application:test
./gradlew presentation:test
```

or everything in once command:
```
./gradlew test
```

## About packages:
You can find the nexts packagees for each layer:

##### domain layer:
```
package myapp.domain
package myapp.test.domain
```

##### infrastructure layer: 
You can find in here classes with **@Repository** tags
```
package myapp.infrastructure
package myapp.test.infrastructure
```

##### presentation layer: 
You will find in here classes with **@ResController** tags
```
package myapp.presentation
package myapp.test.presentation
```

##### application layer: 
You will find in here classes with **@Services** tags
```
package myapp.application
package myapp.test.application
```

## Where is the main (myapp)?
at the root of the project, in /src
This folder just contains a Main.kt, to decouple a bit the framework bootstrap from the rest of the application
 
## TODO:
- introduce kafka producer and consumer in infrastructure layer, using spring-kafka, this will make me to think about how
to work with autoconfiguration in a project with multiple layers, using TDD
