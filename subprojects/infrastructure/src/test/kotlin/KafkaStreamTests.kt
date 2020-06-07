package myapp.test.infrastructure

import myapp.infrastructure.kafkastream.FactoryRepositoryKafkaStream
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TopologyTestDriver
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.shadow.com.univocity.parsers.common.record.RecordFactory
import java.time.Duration
import java.time.Instant
import java.util.*

class KafkaStreamTests {
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

        val inputTopic11: TestInputTopic<String, String> = driver.createInputTopic(
                    inputTopic1,
                    StringSerializer(),
                    StringSerializer(),
                    Instant.now(), Duration.ofSeconds(1)
            )

        inputTopic11.pipeInput("this is my message")
    }


}
