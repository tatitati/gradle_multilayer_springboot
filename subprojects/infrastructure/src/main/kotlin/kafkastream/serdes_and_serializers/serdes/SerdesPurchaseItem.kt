package myapp.infrastructure.kafkastream.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import myapp.infrastructure.kafkastream.pojos.Person
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.BillTotal
import myapp.infrastructure.kafkastream.serdes_and_serializers.pojos.PurchaseItem
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class SerdesPurchaseItem : Serde<PurchaseItem> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
    override fun deserializer(): Deserializer<PurchaseItem> = PurchaseItemDeserializer()
    override fun serializer(): Serializer<PurchaseItem> = PurchaseItemSerializer()
}

class PurchaseItemSerializer : Serializer<PurchaseItem> {
    override fun serialize(topic: String?, data: PurchaseItem?): ByteArray {
        val mapper = ObjectMapper()
        return mapper.writeValueAsString(data).toByteArray()
    }

}

class PurchaseItemDeserializer : Deserializer<PurchaseItem> {
    override fun deserialize(topic: String?, data: ByteArray?): PurchaseItem {
        val mapper = ObjectMapper().registerModule(KotlinModule())
        return mapper.readValue<PurchaseItem>(data, PurchaseItem::class.java)
    }

}
