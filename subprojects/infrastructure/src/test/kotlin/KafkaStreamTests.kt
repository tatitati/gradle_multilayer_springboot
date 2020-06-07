package myapp.test.infrastructure

import myapp.infrastructure.kafkastream.FactoryRepositoryKafkaStream
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.*


class KafkaStreamTests {
    var inputTopic11:TestInputTopic<String, String>? = null
    var outputTopic11: TestOutputTopic<String, String>? = null

    @Test
    fun setupTopologyTestDriver(){
        val properties = Properties()
        properties.apply{
            put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-kafka-stream")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "noneserver:0000")
        }

        val inputTopic1 = "input1"
        val inputTopic2 = "input2"
        val outputTopic1 = "output1"
        val outputTopic2 = "output2"

        val topology = FactoryRepositoryKafkaStream(
                "",
                inputTopic1,
                outputTopic1,
                inputTopic2,
                outputTopic2
        ).buildTopology()

        val driver = TopologyTestDriver(topology, properties)

        inputTopic11 = driver.createInputTopic(inputTopic1, StringSerializer(), StringSerializer())
        outputTopic11 = driver.createOutputTopic(outputTopic1, StringDeserializer(), StringDeserializer())

        inputTopic11!!.pipeInput("this is my message")

        assertEquals(4L, outputTopic11!!.queueSize)
        assertEquals("app1: this", outputTopic11!!.readValue())
        assertEquals("app1: is", outputTopic11!!.readValue())
        assertEquals("app1: my", outputTopic11!!.readValue())
        assertEquals("app1: message", outputTopic11!!.readValue())
    }
}
