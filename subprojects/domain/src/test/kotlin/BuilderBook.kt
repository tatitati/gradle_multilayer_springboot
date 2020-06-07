package myapp.test.domain

import myapp.domain.Book

class BuilderBook {
    var id: Int = Faker.anyInt()
    var name: String = Faker.anyWord()
    var author: String = Faker.anyWord()

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
