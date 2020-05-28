package myapp.applicatin

import org.springframework.stereotype.Service
import myapp.infrastructure.RepositoryBook
import myapp.domain.Book

@Service
class ServiceBook(private val repository: RepositoryBook){

    fun findAllBooks(): List<Book>{
        return repository.findAll()
    }
}
