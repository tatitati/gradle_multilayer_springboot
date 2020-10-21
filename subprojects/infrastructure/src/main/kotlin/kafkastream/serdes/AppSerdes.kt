package myapp.infrastructure.kafkastream.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class AppSerdes : Serde<Person> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
    override fun deserializer(): Deserializer<Person> = PersonDeserializer()
    override fun serializer(): Serializer<Person> = PersonSerializer()
}

class PersonSerializer : Serializer<Person> {
    override fun serialize(topic: String?, data: Person?): ByteArray {
        val mapper = ObjectMapper()
        return mapper.writeValueAsString(data).toByteArray()
    }

}

class PersonDeserializer : Deserializer<Person> {
    override fun deserialize(topic: String?, data: ByteArray?): Person {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        return mapper.readValue<Person>(data, Person::class.java)
    }

}
