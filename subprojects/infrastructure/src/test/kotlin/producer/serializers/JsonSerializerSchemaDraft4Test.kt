package myapp.test.infrastructure.producer.serializers

import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class JsonSerializerSchemaDraft4Test {

    data class Book(val myField1: Int, val myField2: Double, val myField3: String)

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaJsonSchemaSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val book = Book(myField1 = 13, myField2 = 46.8, myField3 = "some text here")
        buildProducer().apply{
            send(ProducerRecord("topic-schema-jsonserializer", book))
            flush()
            close()
        }
    }

    // Without passing any schema this creates the next schema:
    // ========================================================
    //    {
    //        "$schema": "http://json-schema.org/draft-04/schema#",
    //        "additionalProperties": false,
    //        "properties": {
    //        "myField1": {
    //        "type": "integer"
    //    },
    //        "myField2": {
    //        "type": "number"
    //    },
    //        "myField3": {
    //        "oneOf": [
    //        {
    //            "title": "Not included",
    //            "type": "null"
    //        },
    //        {
    //            "type": "string"
    //        }
    //        ]
    //    }
    //    },
    //        "required": [
    //        "myField1",
    //        "myField2"
    //        ],
    //        "title": "Book",
    //        "type": "object"
    //    }
}
