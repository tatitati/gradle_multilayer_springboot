package myapp.infrastructure.kafkastream.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.Friend
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class SerdesFriend : Serde<Friend> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
    override fun deserializer(): Deserializer<Friend> = FriendDeserializer()
    override fun serializer(): Serializer<Friend> = FriendSerializer()
}

class FriendSerializer : Serializer<Friend> {
    override fun serialize(topic: String?, data: Friend?): ByteArray {
        val mapper = ObjectMapper()
        return mapper.writeValueAsString(data).toByteArray()
    }

}

class FriendDeserializer : Deserializer<Friend> {
    override fun deserialize(topic: String?, data: ByteArray?): Friend {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        return mapper.readValue<Friend>(data, Friend::class.java)
    }

}
