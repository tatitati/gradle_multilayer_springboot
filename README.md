
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
\--- Project ':ui' - my UI layer description here
```

## How to run tests

```
./gradlew domain:test
./gradlew infrastructure:test
./gradlew application:test
./gradlew ui:test
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

##### ui layer: 
You will find in here classes with **@ResController** tags
```
package myapp.ui
package myapp.test.ui
```

##### application layer: 
You will find in here classes with **@Services** tags
```
package myapp.application
package myapp.test.application
```
 
## TODO:
- introduce kafka producer and consumer in infrastructure layer, this will make me to think about how
to work with autoconfiguration in a project with multiple layers. And keep adding tests
