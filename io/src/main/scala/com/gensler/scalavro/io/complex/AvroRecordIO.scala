package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.AvroPrimitiveType
import com.gensler.scalavro.types.complex.AvroRecord
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericRecord, GenericData, GenericDatumWriter }
import org.apache.avro.io.EncoderFactory
import com.gensler.scalavro.io.AvroTypeIO.Implicits._

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.{ TypeTag, typeTag }

import java.io.{ InputStream, OutputStream }

case class AvroRecordIO[T](avroType: AvroRecord[T]) extends AvroTypeIO[T]()(avroType.tag) {

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString

  /**
    * Returns the `org.apache.avro.generic.GenericRecord` representation of
    * the supplied object.
    */
  protected[scalavro] def asGeneric[R <: T: TypeTag](obj: R): GenericRecord = {
    val record = new GenericData.Record(avroSchema)

    avroType.fields.foreach { field =>
      extractField(field.fieldType.tag)

      def extractField[V: TypeTag] =
        ReflectionHelpers.productElement[R, V](obj, field.name) match {
          case Some(value) => {
            val io = avroTypeToIO(field.fieldType.asInstanceOf[AvroType[V]])
            record.put(field.name, io.asGeneric(value))
          }
          case None => throw new RuntimeException(
            "Could not extract a value for field [%s]" format field.name
          )
        }

    }

    return record
  }

  /**
    * Writes a binary representation of the supplied object to the supplied
    * stream.
    */
  def write[R <: T: TypeTag](obj: R, stream: OutputStream) {
    try {
      val datumWriter = new GenericDatumWriter[GenericRecord](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(obj, cause)
    }
  }

  /**
    * Reads a binary representation of the underlying Scala type from the
    * supplied stream.
    */
  def read(stream: InputStream) = Try {
    val args = avroType.fields map { field => field.fieldType.read(stream).get }
    ReflectionHelpers.instantiateCaseClassWith(args)(avroType.tag).get
  }

  def writeJson[R <: T: TypeTag](obj: R, stream: OutputStream) = ???

  def readJson(stream: InputStream) = ???

}