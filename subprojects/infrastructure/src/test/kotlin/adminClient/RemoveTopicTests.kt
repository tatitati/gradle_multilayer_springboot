package myapp.test.infrastructure.adminClient

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.junit.jupiter.api.Test
import java.util.*

class RemoveTopicTests {
    @Test
    fun `can remove a topic`(){
        val properties = Properties();
        properties.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        );

        val adminClient: AdminClient = AdminClient.create(properties)
        adminClient.deleteTopics(listOf("whatever")).all().get()
        Thread.sleep(2000)
    }
}
