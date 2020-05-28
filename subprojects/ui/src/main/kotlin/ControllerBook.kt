package myapp.ui

import org.springframework.stereotype.Controller
import org.springframework.ui.Model
import org.springframework.ui.set
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import javax.annotation.PostConstruct

@RestController
class ControllerBook {

    @PostConstruct
    fun whatever(){
        val a = 5
    }

    @GetMapping("/hello")
    fun helloKotlin(): String {
        println("asdfasdf")
        return "hello world"
    }
}
