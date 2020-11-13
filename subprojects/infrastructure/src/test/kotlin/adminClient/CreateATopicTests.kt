package myapp.test.infrastructure.adminClient

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.junit.jupiter.api.Test
import java.util.*


class CreateATopicTests {
    @Test
    fun `can create a topic`(){
        val properties = Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        val adminClient: AdminClient = AdminClient.create(properties)
        // NewTopic(topicName, numPartitions, replicationFactor)
        val newTopic = NewTopic("topicName", 2, 1.toShort())


        val newTopics: MutableList<NewTopic> = ArrayList<NewTopic>()
        newTopics.add(newTopic)

        adminClient.createTopics(newTopics)
        adminClient.close()
    }
}
