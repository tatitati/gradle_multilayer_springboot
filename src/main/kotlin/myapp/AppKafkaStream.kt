package myapp

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication

@SpringBootApplication
class AppKafkaStream

fun main(args: Array<String>) {
    SpringApplication.run(AppKafkaStream::class.java, *args)
}
