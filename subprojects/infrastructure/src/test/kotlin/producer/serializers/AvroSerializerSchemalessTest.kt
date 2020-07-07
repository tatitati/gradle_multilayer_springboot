package myapp.test.infrastructure.producer.serializers

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DatumWriter
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class AvroSerializerSchemalessTest {

    fun buildProducer(): KafkaProducer<String, String> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", KafkaAvroSerializer::class.java)
            put("schema.registry.url", "http://127.0.0.1:8081")
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val topic = "schemaless"
        val mystring = "=> name = wolf, country = spain"

        // we didn't specify an schema, however the schema is also created and only contains "string"
        buildProducer().apply{
            send(ProducerRecord(topic, mystring))
            flush()
            close()
        }
    }
}
