package myapp.test.infrastructure.schemaregistry

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

class AvroTest {

    @Test
    fun testBatching(){
        val schemaPerson = Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Person",
              "namespace": "myapp.infrastructure",
              "fields": [
                {
                  "name": "firstName",
                  "type": "string"
                },
                {
                  "name": "lastName",
                  "type": "string"
                },
                {
                  "name": "age",
                  "type": "int"
                }
              ]
            }
        """.trimIndent())

        val genericRecordPerson: GenericRecord = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 5)
        }.build()

        assertEquals("""{"firstName": "sam", "lastName": "dedios", "age": 5}""", genericRecordPerson.toString())
    }

    @Test
    fun testBatching2(){
        val schemaPerson = Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Person",
              "namespace": "myapp.infrastructure",
              "fields": [
                {
                  "name": "firstName",
                  "type": "string"
                },
                {
                  "name": "lastName",
                  "type": "string"
                },
                {
                  "name": "age",
                  "type": "int"
                }
              ]
            }
        """.trimIndent())

        val genericRecordPerson: GenericRecord = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 5)
        }.build()

        val avroWriter = GenericDatumWriter<GenericRecord>(schemaPerson)
        val dataFileWriter = DataFileWriter(avroWriter)
        dataFileWriter.create(schemaPerson, File("person_generic_record.avro"))
        dataFileWriter.append(genericRecordPerson)

        // content of person_generic_record.avro
        // Objavro.schema�{"type":"record","name":"Person","namespace":"myapp.infrastructure","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"}]} =��+|$��y�%��
    }
}
