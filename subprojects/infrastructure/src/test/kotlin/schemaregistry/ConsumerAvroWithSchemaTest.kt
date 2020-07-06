package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.*

class ConsumerAvroWithSchemaTest {
    fun buildConsumer(): KafkaConsumer<String, GenericRecord> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
            put("key.deserializer", IntegerDeserializer::class.java)
            put("value.deserializer", KafkaAvroDeserializer::class.java)
            put("group.id", "mygroup")
            put("auto.offset.reset", "earliest")
            put("schema.registry.url", "http://127.0.0.1:8081")
        }
        val topic = "my-generic-record-value"

        val consumer = KafkaConsumer<String, GenericRecord>(properties)
        consumer.subscribe(listOf(topic))
        return consumer
    }

    // You can run the producer in the CLI with:
    // kafka-avro-console-producer --broker-list $khost --topic my-generic-record-value --property schema.registry.url=http://127.0.0.1:8081 --property value.schema='{"type":"record","name":"Person","namespace":"myapp.infrastructure","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"}]}'
    // {"firstName": "sam", "lastName": "dedios", "age": 55555}
    @Test
    fun testBatching(){
        val avroConsumer: KafkaConsumer<String, GenericRecord> = buildConsumer()

        while (true) {
            val batchOfRecords: ConsumerRecords<String, GenericRecord> = avroConsumer.poll(Duration.ofSeconds(3))
            batchOfRecords.iterator().forEach {
                println("\n\n\n\n\n\n\n =========> Partition: " + it.partition() + ", Offset: " + it.offset() + ", Key: " + it.key() + ", Value: " + it.value())
            }
        }
    }
}
