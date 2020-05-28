package myapp.ui

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import myapp.applicatin.ServiceBook
import myapp.domain.Book

@RestController
class ControllerBook @Autowired constructor(
    val service: ServiceBook
){
    // URL: localhost:8080/hello
    @GetMapping("/hello")
    fun helloKotlin(): String {
        val books: List<Book> = service.findAllBooks()
        return books.toString()
    }
}
