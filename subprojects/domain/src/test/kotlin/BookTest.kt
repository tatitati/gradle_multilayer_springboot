package myapp.test.domain

import myapp.domain.Book
import myapp.test.domain.builders.BuilderBook
import org.junit.jupiter.api.Test

class BookTest {

    @Test
    fun `can create a book`(){
        Book(343, "asdfadf", "asdfasd")
        assert(2 == 2)
    }

    @Test
    fun `can create a book using builders`(){
        val book = BuilderBook().anyWithName("myname").build()
        assert("myname" == book.bookName)
    }

}
