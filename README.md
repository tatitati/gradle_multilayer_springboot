# TODO:
- Add CLI endpoints at presentation level to have an example of a CLI
- Get better with profiles. What happen if I add another application.yml into tests/resources?, it seems that override the one in /src?, but just the ones we specify?
- Create an example of Ktable and tesst it
- Create an example to play with KSQL
- Improve knowledge about the types we use for keys, values, serdes and se/de-serializers
- Investigate how JARs are created and packaged in a multiprojects with shared fixtures in tests. It will be interesting


## Start zookeeper + kafka + schema registry + landoop

```
docker-compose up -d
open http://127.0.0.1:3030
```

# Run confluent Avro tools:

```
docker run  -it --rm  --net=host  confluentinc/cp-schema-registry:3.1.1 bash <kafka-avro-console-consumer.....>
```

or you can:

```
hook_avro.sh  <avro command>
```

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

# AVRO FILES
Some useful commands to play with Avro files:

#### Read avro file as json:
```
$ avro-tools tojson --pretty subprojects/infrastructure/person_generic_record.avro
{
    "firstName" : "sam",
    "lastName" : "dedios",
    "age" : 66
}
```

#### Get schema from an avro file:
```
   $ avro-tools getschema subprojects/infrastructure/person_generic_record.avro
   {
       "type" : "record",
       "name" : "Person",
       "namespace" : "myapp.infrastructure",
       "fields" : [ 
            {
               "name" : "firstName",
               "type" : "string"
            }, {
               "name" : "lastName",
               "type" : "string"
            }, {
               "name" : "age",
               "type" : "int"
            } 
        ]
   }
```
# AVRO CONSUMER/PRODUCER

### setup
```
docker-compose up -d
```

### produce some messages + create topic + create schema in schema registry + validate produced messages (all with an avro-producer)
```
kafka-avro-console-producer --broker-list $khost --topic test-avro --property schema.registry.url=http://127.0.0.1:8081 --property value.schema='{"type":"record", "name": "myrecord", "fields": [{"name": "f1", "type": "string"}, {"name": "f2", "type": "int", "default": 0}]}'


{"f1":"evolution", "f2":2}
```

### Consume messages with an avro-consumer
```
kafka-avro-console-consumer --topic test-avro-from-local --bootstrap-server $khost --property schema.registry.url=http://127.0.0.1:8081
```
