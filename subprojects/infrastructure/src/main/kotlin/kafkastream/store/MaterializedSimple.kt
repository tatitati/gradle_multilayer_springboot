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
        put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stateful-with-store")
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
    }

    // FIXTURES
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
            val purchaseItem = PurchaseItem(
                    firstName = FakerSimple.anyOf(listOf("Michael", "John", "Raul", "Francisco")),
                    price = FakerSimple.anyOf(listOf(1, 200, 3000, 40000))
            )
            val futureResult = producer.send(ProducerRecord(
                    topicInput, // topic
                    purchaseItem.firstName, // key
                    jsonMapper.writeValueAsString(purchaseItem) // value
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

        val inputPurchaseItems: KStream<String, PurchaseItem> = builder.stream(topicInput, serdesSource)
        val statefulAccumulatedBills: KStream<String, BillTotal> = inputPurchaseItems
                .transformValues(
                        ValueTransformerSupplier { MyValueTransformer() },
                        storeName
                )
        statefulAccumulatedBills.to(topicOutput, serdesSink)

        // START
        val topology: Topology = builder.build()
        val streams = KafkaStreams(topology, prop)
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }
}

// LETS USE OUR STORE! :) , THIS IS WHAT MAKE OUR KAFKA STREAM STATEFUL: Inputs are Purachase Items, and Outputs are BillTotal
class MyValueTransformer: ValueTransformer<PurchaseItem, BillTotal> {
    lateinit var stateStore: KeyValueStore<String, Int>

    override fun init(context: ProcessorContext?) {
        this.stateStore = context!!.getStateStore("mystore") as KeyValueStore<String, Int>
    }

    override fun transform(value: PurchaseItem?): BillTotal {
        val accumulatedBill = stateStore.get(value!!.firstName) ?: 0
        val totalAccumulated = accumulatedBill + value.price
        stateStore.put(value!!.firstName, totalAccumulated) // we use firstname as our key

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
