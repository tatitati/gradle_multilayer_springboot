package myapp.infrastructure.kafkastream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.time.Duration
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class MaterializedView {
    val prop: Properties = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "materializedview-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    }
    val builder = StreamsBuilder()

    @PostConstruct
    fun run(){
        val input: KStream<String, String> = builder.stream("materializedview_input", Consumed.with(Serdes.String(), Serdes.String()))

        val aggregated: KTable<Windowed<String>, Long> = input
                .mapValues { textLine -> textLine.toLowerCase() }
                .flatMapValues { loweredCase -> loweredCase.split(" ") }
                .selectKey { key, word -> word }
                .groupBy{key: String, value: String -> value}
                // groups records with the same key in windows of 20 seconds
                .windowedBy(
                        TimeWindows.of(Duration.ofSeconds(20)))
                // count recods with the same value in each window
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .suppress(Suppressed.untilWindowCloses(unbounded()).withName("mycounter"))

        aggregated.toStream { windowedKey, _ -> windowedKey.key() }
                // we have a windowed key, so we cannot send this directly to another topic. Cause of that we map to set the real key of the window
                .peek { key: String, value: Long -> println("Key ${key} and value ${value}") }
                .to("materializedview_output", Produced.with(Serdes.String(), Serdes.Long()))

        // start
        val streams = KafkaStreams(builder.build(), prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<MaterializedView>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
