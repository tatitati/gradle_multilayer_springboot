package myapp.ui

import myapp.applicatin.ServiceBook
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.ui.set
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import javax.annotation.PostConstruct

@RestController
class ControllerBook @Autowired constructor(
    val service: ServiceBook
){
    @GetMapping("/hello")
    fun helloKotlin(): String {
        return service.findAllBooks().toString()
    }
}
