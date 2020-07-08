package myapp.test.infrastructure

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import myapp.test.infrastructure.schemaregistry.ProducerUsingJsonSchemaDraft7Test
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import java.io.IOException
import java.io.StringWriter

class CustomJsonSerializerTest {
    data class Pupil(val myField1: Int, val myField2: Double, val myField3: String)

    class MyCustomSerializer @JvmOverloads constructor(t: Class<Pupil?>? = null) : StdSerializer<Pupil>(t) {
        @Throws(IOException::class, JsonProcessingException::class)
        override fun serialize(value: Pupil, jgen: JsonGenerator, provider: SerializerProvider) {
            jgen.writeStartObject()
            jgen.writeNumberField("myField11111111", value.myField1)
            jgen.writeNumberField("myField2", value.myField2)
            jgen.writeStringField("myField3", value.myField3)
            jgen.writeEndObject()
        }
    }

    @Test
    fun asdfasdf(){
        val pupil = Pupil(myField1 = 4, myField2 = 56.7, myField3 = "some text here")

        // register serializer
        val mapper = ObjectMapper()
        val module = SimpleModule()
        module.addSerializer(Pupil::class.java, MyCustomSerializer())
        mapper.registerModule(module)

        val sw = StringWriter()
        mapper.writeValue(sw, pupil)
        println(sw.toString()) // {"myField11111111":4,"myField2":56.7,"myField3":"some text here"}
    }
}
