package myapp.test.infrastructure.KafkaStreams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes.SerdesFriend
import myapp.infrastructure.kafkastream.serdes.SerdesPerson
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.Friend
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.ProcessorContext
import org.junit.jupiter.api.Test
import java.util.*

class TransformerSupplierTest {
    val builder = StreamsBuilder()
    val topicInput = "kstream_inputXXX"
    val topicOutput = "kstream_outputXXX"
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-applicationXX")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
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

        for(i in 1..10) {
            val person = Person(
                    firstName = "firstname"+i,
                    lastName = "lastName"+i,
                    age = 34
            )
            val futureResult = producer.send(ProducerRecord(
                    topicInput,
                    jsonMapper.writeValueAsString(person)
            ))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    @Test
    fun run(){
        this.fixtures()

        val serdesSource: Consumed<String, Person> = Consumed.with(Serdes.String(), SerdesPerson())
        val serdesSink: Produced<String, Friend> = Produced.with(Serdes.String(), SerdesFriend())

        val mystream: KStream<String, Person> = builder.stream<String, Person>(topicInput, serdesSource)

        val processed: KStream<String, Friend> = mystream.transform(TransformerSupplier {PreferencesTransformer()})
        processed.to(topicOutput, serdesSink)

        val streams = KafkaStreams(builder.build(), prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
        Thread.sleep(10000)
    }
}

class PreferencesTransformer: Transformer<String, Person, KeyValue<String, Friend>> {
    override fun init(context: ProcessorContext) {}

    override fun transform(key: String?, record: Person): KeyValue<String, Friend> {
        val friend = Friend(
                firstName = record.firstName,
                lastName = record.lastName
        )
        return KeyValue.pair(record.firstName, friend)
    }

    override fun close() {}
}

