package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.json.JsonSchema
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.junit.jupiter.api.Test

class SchemaRegistryClientTest {

    @Test
    fun basictest(){
        val client = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

//        val allSubjects = client.allSubjects
//        println(allSubjects) // [mydraft7-value, topic-kafkajsonschemaserializer-value, topic-kafkajsonschemaserializer-value]
//
//        val allSchemaVersions = client.getAllVersions("mydraft7-value")
//        println(allSchemaVersions) // [1, 2]
//
//        val mode1 = client.getAllSubjectsById(1)
//        val mode2 = client.getAllSubjectsById(2)
//        val mode3 = client.getAllSubjectsById(3)
//        println(mode1) // [mydraft7-value, topic-kafkajsonschemaserializer-value]
//        println(mode2) // [mydraft7-value]
//        println(mode3) // [topic-withavrorecord-schema-value]
//
//        val avroSchema = client.getSchemaById(3) // {"type":"record","name":"value_jsons_serializer_schemaless","namespace":"com.mycorp.mynamespace","doc":"Sample schema to help you get started.","fields":[{"name":"myField1","type":"int","doc":"The int type is a 32-bit signed integer."},{"name":"myField2","type":"double","doc":"The double type is a double precision (64-bit) IEEE 754 floating-point number."},{"name":"myField3","type":"string","doc":"The string is a unicode character sequence."}]}
//        println(avroSchema)


//        val jsonSchema = client.getSchemaById(2) // {"type":"record","name":"value_jsons_serializer_schemaless","namespace":"com.mycorp.mynamespace","doc":"Sample schema to help you get started.","fields":[{"name":"myField1","type":"int","doc":"The int type is a 32-bit signed integer."},{"name":"myField2","type":"double","doc":"The double type is a double precision (64-bit) IEEE 754 floating-point number."},{"name":"myField3","type":"string","doc":"The string is a unicode character sequence."}]}
//        println(jsonSchema)


        val sche = "\$schema"
        val id = "\$id"
        val rawSchema = """
            {
              "$sche": "http://json-schema.org/draft-07/schema#",
              "$id": "http://example.com/myURI.schema.json",
              "type": "object",
              "properties": {
                "field1": { "type": "string" }
              },
              "additionalProperties":  { "type": "string" }
            }
        """.trimIndent()

        val rawSchemaEvolution = """
            {
              "$sche": "http://json-schema.org/draft-07/schema#",
              "$id": "http://example.com/myURI.schema.json",
              "type": "object",
              "properties": {
                "field1": { "type": "string" },
                "field2": { "type": "string" }
              },
              "additionalProperties":  { "type": "string" }
            }
        """.trimIndent()

        val schemaObject = JSONObject(JSONTokener(rawSchemaEvolution))
        val schemaLoaded = SchemaLoader.load(schemaObject)

        val schema = JsonSchema(schemaLoaded)

        schema.validate()
        println(schema.schemaType()) // JSON
        println(schema.references()) // []
        println(schema.version())
        val result = client.register("mynewsubject", schema)
        println(result) // 12

    }
}
