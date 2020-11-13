package myapp.test.infrastructure.adminClient

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.ListTopicsResult
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.*

class CheckIfTopicExistsTests {
    @Test
    fun `check if topic exists`(){
        val props = Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        val client: AdminClient = AdminClient.create(props)
        val options = ListTopicsOptions()
        options.listInternal(true); // includes internal topics such as __consumer_offsets
        val topics: ListTopicsResult = client.listTopics(options)
        val currentTopicList: Set<String> = topics.names().get()

        assertTrue(currentTopicList.contains("__consumer_offsets"))
    }
}
