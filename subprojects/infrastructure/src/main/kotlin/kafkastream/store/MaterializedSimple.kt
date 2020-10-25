package myapp.infrastructure.kafkastream.store

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes.SerdesPerson
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
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

    val builder = StreamsBuilder()
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")

    }

    fun sendToTopicSomeUsers(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)

        for(i in 1..10) {
            val person = Person(
                    firstName = "firstname",
                    lastName = "lastName",
                    age = 34
            )
            val futureResult = producer.send(ProducerRecord(
                    "kstream_inputXX",
                    jsonMapper.writeValueAsString(person)
            ))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }


    @PostConstruct
    fun run(){
        this.sendToTopicSomeUsers()

        val serdesSource: Consumed<String, Person>   = Consumed.with(Serdes.String(), SerdesPerson())
        val serdesSink: Produced<String, Person> = Produced.with(Serdes.String(), SerdesPerson())=

        val ks0: KStream<String, Person> = builder.stream("materializedsimple_input", serdesSource)
        val ks0WithKey: KStream<String, Person> = ks0.selectKey { key, person -> person.firstName }

        // CREATE STORE
        val mystore: StoreBuilder<KeyValueStore<String, Int>> = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("mystore"),
                Serdes.StringSerde(), Serdes.IntegerSerde())
        builder.addStateStore(mystore)


        ks0WithKey.transformValues(ValueTransformerSupplier { MyValueTransformer() })


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

class MyValueTransformer: ValueTransformer<Person, KeyValue<String, Person>> {
    override fun init(context: ProcessorContext?) {
        TODO("Not yet implemented")
    }

    override fun transform(value: Person?): KeyValue<String, Person> {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }


}

fun main(args: Array<String>) {
    runApplication<MaterializedSimple>(*args)

    // CLI for consumer:
    //    kafka-console-consumer --bootstrap-server $khost --topic output_topic --group mygroup --property  key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property  value.deserializer=org.apache.kafka.common.serialization.LongDeserializer --property print.key=true --property print.value=true
}
