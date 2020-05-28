package myapp.applicatin

import myapp.infrastructure.RepositoryBook
import org.springframework.stereotype.Repository
import org.springframework.stereotype.Service
import myapp.domain.Book

@Service
class ServiceBook(private val repository: RepositoryBook){

    fun findAllBooks(): List<Book>{
        return repository.findAll()
    }
}
