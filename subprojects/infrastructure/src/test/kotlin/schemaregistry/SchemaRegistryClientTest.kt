package myapp.test.infrastructure.schemaregistry

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.junit.jupiter.api.Test

class SchemaRegistryClientTest {

    data class Book(val myField1: Int, val myField2: Double, val myField3: String)
//
//    fun buildProducer(): KafkaProducer<String, Book> {
//        val client = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)
//        val bookSerializer = KafkaJsonSchemaSerializer<Book>(client)
//        val stringSerializer = KafkaJsonSchemaSerializer<String>(client)
//        val properties = Properties().apply{
//            put("bootstrap.servers", "localhost:9092")
//            put("key.serializer", IntegerSerializer::class.java)
//            put("value.serializer", KafkaJsonSchemaSerializer::class.java)
//            put("schema.registry.url", "http://127.0.0.1:8081")
//        }
//
//        return KafkaProducer(properties, stringSerializer, bookSerializer)
//    }


    @Test
    fun basictest(){
        val client = CachedSchemaRegistryClient("http://127.0.0.1:8081", 1000)

        val allSubjects = client.allSubjects
        println(allSubjects) // [mydraft7-value, topic-kafkajsonschemaserializer-value]

        val allSchemaVersions = client.getAllVersions("mydraft7-value")
        println(allSchemaVersions) // [1, 2]

        val mode1 = client.getAllSubjectsById(1)
        val mode2 = client.getAllSubjectsById(2)
        println(mode1) // [mydraft7-value, topic-kafkajsonschemaserializer-value]
        println(mode2) // [mydraft7-value]

    }
}
