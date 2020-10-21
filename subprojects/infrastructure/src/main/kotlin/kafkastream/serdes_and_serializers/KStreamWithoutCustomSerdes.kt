package myapp.infrastructure.kafkastream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
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
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class KStreamWithoutCustomSerdes {
    val jsonMapper = ObjectMapper().apply {
        registerKotlinModule()
        disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        setDateFormat(StdDateFormat())
    }

    fun sendToTopicSomeUsers(){
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
                    birthDate = Date(2020, 3, 15)
            )
            val futureResult = producer.send(ProducerRecord("topic-input-person", jsonMapper.writeValueAsString(person)))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }



    @PostConstruct
    fun run(){
        this.sendToTopicSomeUsers()

        val streamsBuilder = StreamsBuilder()
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        }

        val personJsonStream: KStream<String, String> = streamsBuilder.stream<String, String>(
                "topic-input-person",
                Consumed.with(Serdes.String(), Serdes.String()))

        // convert to Person each json event
        val personStream: KStream<String, Person> = personJsonStream.mapValues { v ->
            jsonMapper.readValue(v, Person::class.java)
        }

        // change the key of each Person
        val resStream: KStream<String, String> = personStream.map { _, p ->
            val birthDateLocal = p.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
            val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
            KeyValue("${p.firstName} ${p.lastName}", "$age")
        }

        resStream.to("topic-output-person", Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()
        val streams = KafkaStreams(topology, props)
        streams.start()

        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<KStreamWithoutCustomSerdes>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
