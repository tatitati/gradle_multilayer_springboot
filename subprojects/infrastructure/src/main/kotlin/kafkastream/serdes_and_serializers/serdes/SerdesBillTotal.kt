package myapp.infrastructure.kafkastream.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.BillTotal
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class SerdesBillTotal : Serde<BillTotal> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
    override fun deserializer(): Deserializer<BillTotal> = BillTotalDeserializer()
    override fun serializer(): Serializer<BillTotal> = BillTotalSerializer()
}

class BillTotalSerializer : Serializer<BillTotal> {
    override fun serialize(topic: String?, data: BillTotal?): ByteArray {
        val mapper = ObjectMapper()
        return mapper.writeValueAsString(data).toByteArray()
    }

}

class BillTotalDeserializer : Deserializer<BillTotal> {
    override fun deserialize(topic: String?, data: ByteArray?): BillTotal {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        return mapper.readValue<BillTotal>(data, BillTotal::class.java)
    }

}
