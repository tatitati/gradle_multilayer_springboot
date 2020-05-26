package infrastructure

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.kafka.config.TopicBuilder

@Configuration
@Profile("local")
class AutoCreateConfig {

    @Bean
    fun createNewTopic(): NewTopic {
       return TopicBuilder
                .name("library-events")
                .partitions(1)
                .replicas(1)
                .build()
    }
}
