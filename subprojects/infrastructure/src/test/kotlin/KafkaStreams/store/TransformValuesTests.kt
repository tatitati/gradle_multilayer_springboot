package myapp.test.infrastructure.KafkaStreams.store

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import myapp.infrastructure.kafkastream.serdes.SerdesBillTotal
import myapp.infrastructure.kafkastream.serdes.SerdesPurchaseItem
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.BillTotal
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.PurchaseItem
import myapp.test.domain.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.kstream.ValueTransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.junit.jupiter.api.Test
import java.util.*

class TransformValuesTests {
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
                    firstName = Faker.anyOf(listOf("Michael", "John", "Raul", "Francisco")),
                    price = Faker.anyOf(listOf(1, 200, 3000, 40000))
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


    @Test
    fun run(){
        this.sendSomePurchaseItems()

        // CREATE STORE
        val mystore: StoreBuilder<KeyValueStore<String, Int>> = Stores
                .keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(storeName),
                        Serdes.StringSerde(), Serdes.IntegerSerde())
        builder.addStateStore(mystore)

        // KAFKA STREAMS
        val serdesSource: Consumed<String, PurchaseItem> = Consumed.with(Serdes.String(), SerdesPurchaseItem())
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
        Thread.sleep(10000)
    }
}

// LETS USE OUR STORE! :) , THIS IS WHAT MAKE OUR KAFKA STREAM STATEFUL: Inputs are Purachase Items, and Outputs are BillTotal
class MyValueTransformer: ValueTransformer<PurchaseItem, BillTotal> {
    lateinit var stateStore: KeyValueStore<String, Int>

    override fun init(context: ProcessorContext?) { // If I implement just Transformer, then I don't have any context that allow me to access to the store
        this.stateStore = context!!.getStateStore("mystore") as KeyValueStore<String, Int>
    }

    override fun transform(value: PurchaseItem?): BillTotal {
        val accumulatedBill = stateStore.get(value!!.firstName) ?: 0
        val totalAccumulated = accumulatedBill + value.price
        stateStore.put(value!!.firstName, totalAccumulated) // we use firstname as our key in the store

        return BillTotal(value!!.firstName, totalAccumulated)
    }

    override fun close() {
        TODO("Not yet implemented")
    }
}

