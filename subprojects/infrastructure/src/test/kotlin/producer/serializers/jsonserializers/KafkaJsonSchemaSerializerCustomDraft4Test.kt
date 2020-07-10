package myapp.test.infrastructure.producer.serializers.jsonserializers

import io.confluent.kafka.formatter.json.JsonSchemaMessageReader
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class KafkaJsonSchemaSerializerCustomTest {

    data class Book(val myField1: Int, val myField2: Double, val myField3: String)

    class CustomSerializer<Book>(): KafkaJsonSchemaSerializer<Book>() {
        override fun configure(config: Map<String?, *>?, isKey: Boolean) {
            val configSerializer = Properties().apply{
                put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
                put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_DOC, "http://127.0.0.1:8081,http://127.0.0.1:809")
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")
            }

            this.configure(
                    KafkaJsonSchemaSerializerConfig(configSerializer)
            )
        }
    }

    fun buildProducer(): KafkaProducer<String, Book> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", CustomSerializer::class.java)
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val book = Book(myField1 = 13, myField2 = 46.8, myField3 = "some text here")
        buildProducer().apply{
            send(ProducerRecord("mydraft8", book)).get()
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
