package myapp.infrastructure.kafkastream

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Produced
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class KStreamSimple {
    val builder = StreamsBuilder()
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    }

    @PostConstruct
    fun run(){
        val wordCountInput: KStream<String, String> = builder.stream("kstream_input", Consumed.with(Serdes.String(), Serdes.String()))
        val wordCountsResult: KStream<String, String> = wordCountInput
                .mapValues { textLine -> textLine.toLowerCase() }
                .flatMapValues { loweredCase -> loweredCase.split(" ") }
                .selectKey { key, word -> word }
                .peek{key, value -> println("KEY: $key,\tVALUE: $value")}

        wordCountsResult.to("kstream_output", Produced.with(Serdes.String(), Serdes.String()))
        val streams = KafkaStreams(builder.build(), prop)

        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<KStreamSimple>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
