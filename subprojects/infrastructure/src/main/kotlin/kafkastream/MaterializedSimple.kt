package myapp.infrastructure.kafkastream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.time.Duration
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class MaterializedSimple {
    val prop: Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    }
    val builder = StreamsBuilder()

    @PostConstruct
    fun run(){
        val input: KStream<String, String> = builder.stream("materializedsimple_input", Consumed.with(Serdes.String(), Serdes.String()))

        val aggregated = input
                .mapValues { textLine -> textLine.toLowerCase() }
                .flatMapValues { loweredCase -> loweredCase.split(" ") }
                .selectKey { key, word -> word }
                .groupBy{key: String, value: String -> value}
                .count(Materialized.`as`("mycount"))
                .filter { key: String, value: Long ->  key == "pink" } // this doesnt work????, why>??


        aggregated.toStream()
                // we have a windowed key, so we cannot send this directly to another topic. Cause of that we map to set the real key of the window
                .peek { key: String, value: Long -> println("${key} | ${value}") }
                .to("materializedsimple_output", Produced.with(Serdes.StringSerde(), Serdes.Long()))

        // start
        val streams = KafkaStreams(builder.build(), prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<MaterializedSimple>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
