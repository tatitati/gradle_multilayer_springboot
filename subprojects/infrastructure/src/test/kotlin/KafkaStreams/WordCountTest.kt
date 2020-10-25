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
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Produced
import org.junit.jupiter.api.Test
import java.util.*

class WordCountTest {
    val builder = StreamsBuilder()
    val topicInput = "input_topic"
    val topicOutput = "output_topic"
    val serdesSource =  Consumed.with(Serdes.String(), Serdes.String())
    val serdesSink = Produced.with(Serdes.String(), Serdes.Long())
    val streamProps = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
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

        for (i in 1..10) {
            val words = Faker.anyOf(listOf(
                    "orange pink red",
                    "blue pink pink pink",
                    "pink red red red pink blue",
                    "red orange orange red blue"
            ))

            val futureResult = producer.send(ProducerRecord(
                    topicInput,
                    jsonMapper.writeValueAsString(words)
            ))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    @Test
    fun run(){
        this.fixtures()

        val wordCountInput: KStream<String, String> = builder.stream(topicInput, serdesSource)

        val wordCountsResult: KTable<String, Long> = wordCountInput
                .mapValues { textLine -> textLine.toLowerCase() }
                .flatMapValues { loweredCase -> loweredCase.split(" ") }
                .selectKey { key, word -> word }
                .peek{key, value -> println("=====>KEY: $key,\tVALUE: $value")}
                .groupByKey()
                .count()
        
        wordCountsResult.toStream().to(topicOutput, serdesSink)

        val streams = KafkaStreams(builder.build(), streamProps)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
        Thread.sleep(10000)
    }
}
