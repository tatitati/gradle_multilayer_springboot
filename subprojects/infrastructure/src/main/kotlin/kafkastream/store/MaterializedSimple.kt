package myapp.infrastructure.kafkastream.store

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.serdes.SerdesBillTotal
import myapp.infrastructure.kafkastream.serdes.SerdesPurchaseItem
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.BillTotal
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.PurchaseItem
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.*
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

    val topicInput = "inputXXX"
    val topicOutput = "outputXXX"
    val storeName = "mystore"
    val builder = StreamsBuilder()
    val prop = Properties().apply {
        put(StreamsConfig.APPLICATION_ID_CONFIG, "materialized-application")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")

    }

    fun sendSomePurchaseItems(){
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
        }

        val properties = Properties().apply{
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
        }

        val producer = KafkaProducer<String, String>(properties)


        for (i in 1..10) {
            val anyprice = FakerSimple.anyOf(listOf(1, 200, 3000, 40000))
            val anyuserName = FakerSimple.anyOf(listOf("Michael", "John", "Raul", "Francisco"))

            val purchaseItem = PurchaseItem(
                    firstName = anyuserName,
                    price = anyprice
            )
            val futureResult = producer.send(ProducerRecord(
                    topicInput,
                    jsonMapper.writeValueAsString(purchaseItem)
            ))
            futureResult.get()
        }

        producer.flush()
        producer.close()
    }


    @PostConstruct
    fun run(){
        this.sendSomePurchaseItems()

        // CREATE STORE
        val mystore: StoreBuilder<KeyValueStore<String, Int>> = Stores
                .keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        Serdes.StringSerde(), Serdes.IntegerSerde())
        builder.addStateStore(mystore)

        // KAFKA STREAMS
        val serdesSource: Consumed<String, PurchaseItem>   = Consumed.with(Serdes.String(), SerdesPurchaseItem())
        val serdesSink: Produced<String, BillTotal> = Produced.with(Serdes.String(), SerdesBillTotal())

        val ks0: KStream<String, PurchaseItem> = builder.stream(topicInput, serdesSource)
        val ks0WithKey: KStream<String, PurchaseItem> = ks0.selectKey { key, purchaseItem -> purchaseItem.firstName }

        ks0WithKey
                .transformValues(
                        ValueTransformerSupplier { MyValueTransformer() },
                        storeName
                )
                .to(topicOutput, serdesSink)


        // START
        val topology: Topology = builder.build()
        val streams = KafkaStreams(topology, prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

class MyValueTransformer: ValueTransformer<PurchaseItem, BillTotal> {
    lateinit var stateStore: KeyValueStore<String, Int>

    override fun init(context: ProcessorContext?) {
        this.stateStore = context!!.getStateStore("mystore") as KeyValueStore<String, Int>
    }

    override fun transform(value: PurchaseItem?): BillTotal {
        val accumulatedBill = stateStore.get(value!!.firstName) ?: 0
        val totalAccumulated = accumulatedBill + value.price
        stateStore.put(value!!.firstName, totalAccumulated)

        return BillTotal(
                value!!.firstName,
                totalAccumulated
        )

    }

    override fun close() {
        TODO("Not yet implemented")
    }


}

fun main(args: Array<String>) {
    runApplication<MaterializedSimple>(*args)
}
