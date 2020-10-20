package myapp.infrastructure.kafkastream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.kstream.Produced
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class KStreamSimpleUsers {
    val streamsBuilder = StreamsBuilder()
    val props = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    }

    @PostConstruct
    fun run(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            setDateFormat(StdDateFormat())
        }

        val personJsonStream: KStream<String, String> = streamsBuilder.stream<String, String>(
                "topic-input-person",
                Consumed.with(Serdes.String(), Serdes.String()))

        val personStream: KStream<String, Person> = personJsonStream.mapValues { v ->
            jsonMapper.readValue(v, Person::class.java)
        }

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
    runApplication<KStreamSimpleUsers>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
