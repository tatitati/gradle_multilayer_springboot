
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
