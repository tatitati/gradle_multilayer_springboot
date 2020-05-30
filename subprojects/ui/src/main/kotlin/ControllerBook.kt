package myapp.ui

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import myapp.applicatin.ServiceBook
import myapp.domain.Book
import myapp.infrastructure.consumer.RepositoryConsumer
import myapp.infrastructure.producer.RepositoryProducer

// Examples:
// localhost:8081/hello
// localhost:8081/consume
// localhost:8081/produce


@RestController
class ControllerBook @Autowired constructor(
        val service: ServiceBook,
        val consumer: RepositoryConsumer,
        val producer: RepositoryProducer
){
    @GetMapping("/hello")
    fun helloKotlin(): String {
        val books: List<Book> = service.findAllBooks()
        return books.toString()
    }

    @GetMapping("/consume")
    fun consume(): Unit {
        consumer.consumeFromTopic("mytopic")
    }

    @GetMapping("/produce")
    fun produce(): Unit {
        val book = Book(23, "aaaa", "bbbbb")
        producer.sendToTopic(book, "mytopic")
    }
}
