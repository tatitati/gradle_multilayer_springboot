package myapp.infrastructure.kafkastream.serdes_and_serializers

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
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class KStreamWithCustomSerdes {


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
            val futureResult = producer.send(ProducerRecord("topic-input-person", jsonMapper.writeValueAsString(person)))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }



    @PostConstruct
    fun run(){
        this.fixtures()

        val streamsBuilder = StreamsBuilder()
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        }

        val personStream: KStream<String, Person> = streamsBuilder.stream<String, Person>(
                "topic-input-person",
                Consumed.with(Serdes.String(), SerdesPerson()))

        // change the key of each Person
        val resStream: KStream<String, String> = personStream.map { _, p ->
            KeyValue("${p.firstName} ${p.lastName}", p.age.toString())
        }

        resStream.to("topic-output-person", Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()
        val streams = KafkaStreams(topology, props)
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<KStreamWithCustomSerdes>(*args)
}
