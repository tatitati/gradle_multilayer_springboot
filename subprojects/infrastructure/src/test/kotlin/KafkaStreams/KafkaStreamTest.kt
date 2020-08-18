package myapp.test.infrastructure.KafkaStreams

import myapp.infrastructure.kafkastream.FactoryRepositoryKafkaStream
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaStreamTest {
    var inputTopic:TestInputTopic<String, String>? = null
    var outputTopic: TestOutputTopic<String, String>? = null

    @BeforeAll
    fun start() {
        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
        }

        val repo = FactoryRepositoryKafkaStream(
                "",
                "input1", "output1",
                "input2", "output2")

        val topology = repo.buildTopology()

        val driver = TopologyTestDriver(topology, properties)
        inputTopic = driver.createInputTopic(repo.inputTopic, StringSerializer(), StringSerializer())
        outputTopic = driver.createOutputTopic(repo.outputTopic, StringDeserializer(), StringDeserializer())
    }

    @Test
    fun setupTopologyTestDriver(){
        inputTopic!!.pipeInput("this is my message")

        assertEquals(4L, outputTopic!!.queueSize)
        assertEquals("app1: this", outputTopic!!.readValue())
        assertEquals("app1: is", outputTopic!!.readValue())
        assertEquals("app1: my", outputTopic!!.readValue())
        assertEquals("app1: message", outputTopic!!.readValue())
    }
}
