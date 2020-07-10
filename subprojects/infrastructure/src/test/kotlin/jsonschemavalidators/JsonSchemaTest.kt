package myapp.test.infrastructure.jsonschemavalidators

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
            "$sch": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "city": { "type": "string" },
                "birth": { "type": "string", "format": "date" },
                "iri": { "type": "string", "format": "iri" },
                "idnemail": { "type": "string", "format": "idn-email" },
                "conditional": {
                    "if": {"type": "string"},
                    "then": {"minLength": 3},
                    "else": {"const": 0}
                }
              },
            "required": ["birth", "iri", "idnemail", "conditional"]    
            }
        """.trimIndent()

        val rawSchema = JSONObject(JSONTokener(schemaText))
        val schema = SchemaLoader.load(rawSchema)

        val jsonToValidate = """
                {
                  "birth": "2020-10-20",
                  "iri": "asdf asdf",
                  "idnemail": "1234",
                  "conditional": 0
                }
            """.trimIndent()
        schema.validate(JSONObject(jsonToValidate)) // throws a ValidationException if this object is invalid
    }

}
