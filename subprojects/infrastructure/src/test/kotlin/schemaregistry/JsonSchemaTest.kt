package myapp.test.infrastructure.schemaregistry

import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.junit.jupiter.api.Test

class JsonSchemaTest {
    data  class Mydata(val whatever: String)

    @Test
    fun mytest(){
        val sch = "\$schema"
        val id = "\$id"

        val schemaText = """
            {
            "$sch": "http://json-schema.org/draft-04/schema#",
            "type": "object",
            "properties": {
                "city": { "type": "string" },
                "birth": { "type": "string", "format": "date" },
                "field3": { "type": "string", "format": "idn-email" },
                "field4": {
                    "if": {"type": "string"},
                    "then": {"minLength": 3},
                    "else": {"const": 0}
                }
              },
            "required": ["birth", "field3", "field4"]    
            }
        """.trimIndent()

        val rawSchema = JSONObject(JSONTokener(schemaText))
        val schema = SchemaLoader.load(rawSchema)

        val jsonToValidate = """
                {
                  "birth": "1970-01-01",
                  "field3": "1234",
                  "field4": "aaa"
                }
            """.trimIndent()
        schema.validate(JSONObject(jsonToValidate)) // throws a ValidationException if this object is invalid
    }

}
