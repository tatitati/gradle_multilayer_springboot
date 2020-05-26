package myapp.test.domain

import myapp.domain.Book
import org.junit.jupiter.api.Test

class BookTest {

    @Test
    fun `can create a book`(){
        Book(343, "asdfadf", "asdfasd")
        assert(2 == 2)
    }

}
