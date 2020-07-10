package myapp.test.infrastructure.producer.serializers.jsonserializers

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.kafka.formatter.json.JsonSchemaMessageReader
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.json.JsonSchema
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.serialization.IntegerSerializer
import org.junit.jupiter.api.Test
import java.util.*

class KafkaJsonSchemaSerializerCustomDraft7Test {

    data class Book(val myField1: Int, val myField2: Double, val myField3: String)

    class CustomSerializer<JsonNode>(): KafkaJsonSchemaSerializer<JsonNode>() {
        override fun configure(config: Map<String?, *>?, isKey: Boolean) {
            val configSerializer = Properties().apply{
                put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
                put(KafkaJsonSchemaSerializerConfig.SCHEMA_REGISTRY_URL_DOC, "http://127.0.0.1:8081,http://127.0.0.1:809")
                put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")
            }

            this.configure(
                    KafkaJsonSchemaSerializerConfig(configSerializer)
            )
        }
    }

    fun buildProducer(): KafkaProducer<String, JsonNode> {
        val properties = Properties().apply{
            put("bootstrap.servers", "localhost:9092")
            put("key.serializer", IntegerSerializer::class.java)
            put("value.serializer", CustomSerializer::class.java)
        }

        return KafkaProducer(properties)
    }

    @Test
    fun jsonProducer(){
        val mapper = ObjectMapper()
        val book = Book(myField1 = 13, myField2 = 46.8, myField3 = "some text here")
        val bookJsonNode = mapper.readTree(mapper.writeValueAsString(book))
        val sche = "\$schema"
        val schema = mapper.readTree("""
            {                                                         
                "$sche": "http://json-schema.org/draft-07/schema#", 
                "additionalProperties": false,                        
                "properties": {                                       
                    "myField1": {                                         
                        "type": "integer"                                     
                    },                                                        
                        "myField2": {                                         
                        "type": "number"                                      
                    },                                                        
                    "myField3": {                                         
                        "oneOf": [                                            
                            {                                                     
                                "title": "Not included",                          
                                "type": "null"                                    
                            },                                                    
                            {                                                     
                                "type": "string"                                  
                            }                                                     
                        ]                                                     
                    }                                                         
                },                                                        
                "required": [                                         
                    "myField1",                                           
                    "myField2"                                            
                ],                                                    
                "title": "Book",                                      
                "type": "object"                                      
            }                                                         
        """.trimIndent())
        val envelope = JsonSchemaUtils.envelope(schema, bookJsonNode)
        buildProducer().apply{
            send(ProducerRecord("mydraft10", envelope)).get()
            flush()
            close()
        }
    }
}
