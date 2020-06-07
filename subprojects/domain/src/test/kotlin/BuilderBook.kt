package myapp.test.domain

import myapp.domain.Book

class BuilderBook {
    var id: Int = Faker.anyInt()
    var name: String = Faker.anyString()
    var author: String = Faker.anyString()

    fun anyWithName(name: String): BuilderBook {
        this.name = name
        return this
    }

    fun build(): Book {
        return Book(
            this.id,
            this.name,
            this.author
        )
    }
}
