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

        val allSubjects = client.allSubjects
        val allSchemaVersions = client.getAllVersions("mydraft7-value")
        val jsonSchema = client.getSchemaById(2)
        val schemaVersion = client.getByVersion("mydraft7-value", 1, false)
        println(schemaVersion)
        jsonSchema.validate()


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
        val result = client.register("mynewsubject", schema)
        println(result) // 12

    }
}
