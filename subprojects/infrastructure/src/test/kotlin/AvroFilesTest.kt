package myapp.test.infrastructure

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DatumWriter
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

class AvroFilesTest {

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

    @Test
    fun canBuildAGenericRecord(){
        val genericRecordPerson: GenericRecord = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 5)
        }.build()

        assertEquals("""{"firstName": "sam", "lastName": "dedios", "age": 5}""", genericRecordPerson.toString())
    }

    @Test
    fun canWriteAGenericRecordToAAvroFile(){
        val genericRecordPerson: GenericRecord = GenericRecordBuilder(schemaPerson).apply{
            set("firstName", "sam")subprojects/infrastructure/src/test/kotlin/AvroFilesTest.kt
            set("lastName", "dedios")
            set("age", 66)
        }.build()

        // write to file
        val datumWriter: DatumWriter<GenericRecord> = GenericDatumWriter(schemaPerson)
        val dataFileWriter = DataFileWriter(datumWriter)
        dataFileWriter.create(schemaPerson, File("person_generic_record.avro"))
        dataFileWriter.append(genericRecordPerson)
        dataFileWriter.flush()
        dataFileWriter.close()

        // content of avro file:
        // =====================
        //          Objavro.schema�{"type":"record","name":"Person","namespace":"myapp.infrastructure","fields":[{"name":"firstName","type":"string"},{"name":"lastName","type":"string"},{"name":"age","type":"int"}]} bN�����v&l��\�mgsamdedios�bN�����v&l��\�mg




        // to read avro file as json:
        // =========================
        //        $ avro-tools tojson --pretty subprojects/infrastructure/person_generic_record.avro
        //        {
        //            "firstName" : "sam",
        //            "lastName" : "dedios",
        //            "age" : 66
        //        }



        // Get schema from avro file
        // =========================
        //        $ avro-tools getschema subprojects/infrastructure/person_generic_record.avro
        //        {
        //            "type" : "record",
        //            "name" : "Person",
        //            "namespace" : "myapp.infrastructure",
        //            "fields" : [ {
        //            "name" : "firstName",
        //            "type" : "string"
        //        }, {
        //            "name" : "lastName",
        //            "type" : "string"
        //        }, {
        //            "name" : "age",
        //            "type" : "int"
        //        } ]
        //        }
    }
}
