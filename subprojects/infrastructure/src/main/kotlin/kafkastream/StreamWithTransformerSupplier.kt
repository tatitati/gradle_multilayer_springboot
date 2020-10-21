package myapp.infrastructure.kafkastream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
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
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.ProcessorContext
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class StreamWithTransformerSupplier {
    val builder = StreamsBuilder()
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream-applicationX")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    }

    fun sendToTopicSomeUsers(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            setDateFormat(StdDateFormat())
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
                    birthDate = Date(2020, 3, 15)
            )
            val futureResult = producer.send(ProducerRecord("kstream_inputX", jsonMapper.writeValueAsString(person)))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }

    @PostConstruct
    fun run(){
        this.sendToTopicSomeUsers()

        val serdesSource: Consumed<String, Person>   = Consumed.with(Serdes.String(), SerdesPerson())
        val serdesSink: Produced<String, Person>     = Produced.with(Serdes.String(), SerdesPerson())

        builder
                .stream<String, Person>("kstream_inputX", serdesSource)
                .transform(TransformerSupplier {PreferencesTransformer()})
                .to("kstream_output", serdesSink)

        val streams = KafkaStreams(builder.build(), prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

class PreferencesTransformer: Transformer<String, Person, KeyValue<String, Person>?> {
    override fun init(context: ProcessorContext) {
        println("THIS IS INIT!")
    }

    override fun transform(key: String, record: Person): KeyValue<String, Person>? {
        println("THIS IS TRANSFORM!")
        println(record)
        return KeyValue.pair(key, record)
    }

    override fun close() {
        println("THIS IS CLOSE!")
    }
}


fun main(args: Array<String>) {
    runApplication<StreamWithTransformerSupplier>(*args)
}
