package myapp.infrastructure

import myapp.domain.Book
import org.springframework.stereotype.Repository

@Repository
class RepositoryBook {

    fun findAll(): List<Book>{
        return listOf(
                Book(23, "aaaa", "bbbb"),
                Book(24, "cccc", "dddd")
        )
    }
}
