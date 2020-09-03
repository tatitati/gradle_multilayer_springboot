package myapp.test.infrastructure.KafkaStreams

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Value
import java.util.*

class KafkaStreamOneTest {
    fun buildTopology(): Topology{
        val streamsBuilder = StreamsBuilder()
        streamsBuilder
                .stream("input_topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues { textLine ->
                    textLine.toLowerCase()
                }
                .flatMapValues { loweredCase ->
                    loweredCase.split(" ")
                }
                .mapValues { splitText ->
                    println("app1: $splitText")
                    "app1: $splitText"
                }
                .to("output_topic", Produced.with(Serdes.String(), Serdes.String()))

        val topology: Topology = streamsBuilder.build()
        return topology
    }

    @Test
    fun checkUseOfTestDriver(){
        val topology: Topology = buildTopology()
        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val driver = TopologyTestDriver(topology, properties)
        val inputTopic = driver.createInputTopic("input_topic", StringSerializer(), StringSerializer())
        val outputTopic = driver.createOutputTopic("output_topic", StringDeserializer(), StringDeserializer())

        inputTopic!!.pipeInput("this is my message")

        assertEquals(4L, outputTopic!!.queueSize)
        assertEquals("app1: this", outputTopic!!.readValue())
        assertEquals("app1: is", outputTopic!!.readValue())
        assertEquals("app1: my", outputTopic!!.readValue())
        assertEquals("app1: message", outputTopic!!.readValue())
    }

    @Test
    fun checkRealBehaviour(){
        val topology: Topology = buildTopology()
        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-application")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094")
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        }

        val kstreams = KafkaStreams(topology, properties)
        kstreams.start()
        Thread.sleep(30000);
        kstreams.close();

        // to test this real beahviour:
        //
        // > producer input_topic
        // > consumer output_topic
        // Producer> this is another message
        // consumer>
        //      app1: this
        //      app1: is
        //      app1: another
        //      app1: message
    }
}
