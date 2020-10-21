package myapp.infrastructure.kafkastream.store

import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes.SerdesPerson
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import javax.annotation.PostConstruct




@SpringBootApplication
class MaterializedSimple {

    private fun getProps(): Properties {
        return Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde())
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde())
        }
    }

    @PostConstruct
    fun run(){
        val builder = StreamsBuilder()
        val ks0: KStream<String, Person> = builder.stream("materializedsimple_input", Consumed.with(Serdes.String(), SerdesPerson()))

        val ksowithKey: KStream<String, Person> = ks0
                .selectKey { key, person -> person.firstName }

        // create store
        val mystore: StoreBuilder<KeyValueStore<String, Long>> = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("mystore"),
                Serdes.StringSerde(), Serdes.LongSerde())

        builder.addStateStore(mystore)


//        ksowithKey.transform()


//        ksowithKey.toStream()
//                // we have a windowed key, so we cannot send this directly to another topic. Cause of that we map to set the real key of the window
//                .peek { key: String, value: Long -> println("${key} | ${value}") }
//                .to("materializedsimple_output", Produced.with(Serdes.StringSerde(), Serdes.Long()))

        // start
//        val topology: Topology = builder.build()
//        val streams = KafkaStreams(topology, this.getProps())
//        streams.start()
//        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

fun main(args: Array<String>) {
    runApplication<MaterializedSimple>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
