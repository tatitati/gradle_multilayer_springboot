package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider
import org.junit.jupiter.api.Test

class SchemaRegistryClientTest {

    @Test
    fun basictest(){
        val client = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

        val allSubjects = client.allSubjects
        println(allSubjects) // [mydraft7-value, topic-kafkajsonschemaserializer-value, topic-kafkajsonschemaserializer-value]

        val allSchemaVersions = client.getAllVersions("mydraft7-value")
        println(allSchemaVersions) // [1, 2]

        val mode1 = client.getAllSubjectsById(1)
        val mode2 = client.getAllSubjectsById(2)
        val mode3 = client.getAllSubjectsById(3)
        println(mode1) // [mydraft7-value, topic-kafkajsonschemaserializer-value]
        println(mode2) // [mydraft7-value]
        println(mode3) // [topic-withavrorecord-schema-value]

        val avroSchema = client.getSchemaById(3) // {"type":"record","name":"value_jsons_serializer_schemaless","namespace":"com.mycorp.mynamespace","doc":"Sample schema to help you get started.","fields":[{"name":"myField1","type":"int","doc":"The int type is a 32-bit signed integer."},{"name":"myField2","type":"double","doc":"The double type is a double precision (64-bit) IEEE 754 floating-point number."},{"name":"myField3","type":"string","doc":"The string is a unicode character sequence."}]}
        println(avroSchema)

        
//        val jsonSchema = client.getSchemaById(2) // {"type":"record","name":"value_jsons_serializer_schemaless","namespace":"com.mycorp.mynamespace","doc":"Sample schema to help you get started.","fields":[{"name":"myField1","type":"int","doc":"The int type is a 32-bit signed integer."},{"name":"myField2","type":"double","doc":"The double type is a double precision (64-bit) IEEE 754 floating-point number."},{"name":"myField3","type":"string","doc":"The string is a unicode character sequence."}]}
//        println(jsonSchema)
    }
}
