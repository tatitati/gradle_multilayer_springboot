package myapp.test.infrastructure.KafkaStreams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.test.domain.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.junit.jupiter.api.Test
import java.util.*

class KafkaStreamSimpleWIthoutToTest {
    val builder = StreamsBuilder()
    val topicInput1 = "kstream_input1"
    val topicInput2 = "kstream_input2"
    val topicOutput = "kstream_output"
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    }

    fun fixtures(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        val topics = listOf(topicInput1, topicInput2)
        for (topicIndex in topics.indices){
            for (i in 1..10) {
                val futureResult = producer.send(ProducerRecord(
                        topics[topicIndex],
                        "TOPIC: ${topics[topicIndex]} \tVALUE: $i"
                ))
                futureResult.get()
            }
        }


        producer.flush()
        producer.close()
    }

    @Test
    fun run(){
        this.fixtures()

        val mystream: KStream<String, String> = builder.stream(listOf(topicInput1, topicInput2), Consumed.with(Serdes.String(), Serdes.String()))
        mystream.peek{key, value -> println("KEY: $key.,\tVALUE: $value")}
//        processed.to(topicOutput, Produced.with(Serdes.String(), Serdes.String()))
        val streams = KafkaStreams(builder.build(), prop)

        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
        Thread.sleep(10000)
    }
}


