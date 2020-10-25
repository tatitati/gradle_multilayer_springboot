package myapp.test.infrastructure.KafkaStreams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes.SerdesPerson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.junit.jupiter.api.Test
import java.util.*
import javax.annotation.PostConstruct

class KStreamWithCustomSerdesTests {
    val topicInput = "topic-input-person"
    val topicOutput = "topic-output-person"

    fun fixtures(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        for(i in 1..10) {
            val person = Person(
                    firstName = "firstname"+i,
                    lastName = "lastName"+i,
                    age = 56
            )
            val futureResult = producer.send(ProducerRecord(topicInput, jsonMapper.writeValueAsString(person)))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    @Test
    fun run(){
        this.fixtures()

        val streamsBuilder = StreamsBuilder()
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        }
        val serdesSource = Consumed.with(Serdes.String(), SerdesPerson())
        val serdesSink = Produced.with(Serdes.String(), Serdes.String())

        val personStream: KStream<String, Person> = streamsBuilder.stream<String, Person>(
                topicInput, serdesSource)

        // change the key of each Person
        val resStream: KStream<String, String> = personStream
                .map { _, p -> KeyValue("${p.firstName} ${p.lastName}", p.age.toString()) }

        resStream.to(topicOutput, serdesSink)

        val topology = streamsBuilder.build()
        val streams = KafkaStreams(topology, props)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))

        Thread.sleep(10000)
    }
}
