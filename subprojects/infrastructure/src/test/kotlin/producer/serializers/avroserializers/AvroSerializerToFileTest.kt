package myapp.test.infrastructure.producer.serializers.avroserializers

import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DatumWriter
import org.junit.jupiter.api.Test
import java.io.File

class AvroSerializerToFileTest {

    @Test
    fun canWriteAGenericRecordToAAvroFile(){
        val schema = Schema.Parser().parse("""
            {
              "type": "record",
              "name": "Person",
              "namespace": "myapp.infrastructure",
              "fields": [
                {"name": "firstName","type": "string"},
                {"name": "lastName","type": "string"},
                {"name": "age","type": "int"}
              ]
            }
        """.trimIndent())

        val genericRecord: GenericRecord = GenericRecordBuilder(schema).apply{
            set("firstName", "sam")
            set("lastName", "dedios")
            set("age", 66)
        }.build()

        val datumWriter: DatumWriter<GenericRecord> = GenericDatumWriter(schema)
        DataFileWriter(datumWriter).apply {
            create(schema, File("person_generic_record.avro"))
            append(genericRecord)
            flush()
            close()
        }

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
