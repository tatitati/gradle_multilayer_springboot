package myapp.test.infrastructure.schemaregistry

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.worldturner.medeia.api.JsonSchemaVersion
import com.worldturner.medeia.api.UrlSchemaSource
import com.worldturner.medeia.api.jackson.MedeiaJacksonApi
import com.worldturner.medeia.schema.validation.SchemaValidator
import org.junit.jupiter.api.Test
import java.io.StringWriter
import java.net.URL

class MaidaValidatorTest {

    data  class Mydata(val whatever: String)

    private val api = MedeiaJacksonApi()
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun loadSchemaFromFileSystem(): SchemaValidator {
        val source = UrlSchemaSource(
                url = URL("http://localhost:8081/schemas/ids/4"),
                version = JsonSchemaVersion.DRAFT07
        )
        val schema = api.loadSchemas(listOf(source))
        return schema
    }

    @Test
    fun loadSchemaOriginal(): SchemaValidator {
        val source = UrlSchemaSource(
                javaClass.getResource("/myschemas/with-default-schema.json")
        )
        val schema = api.loadSchema(source)
        return schema
    }

    @Test
    fun whatever(){
        val validator = loadSchemaFromFileSystem()
        val s = StringWriter()
        val unvalidatedGenerator = objectMapper.factory.createGenerator(s)
        val validatedGenerator = api.decorateJsonGenerator(validator, unvalidatedGenerator)
        objectMapper.writeValue(validatedGenerator, Mydata("gfdgdfsg"))
        println(s)
    }

}
