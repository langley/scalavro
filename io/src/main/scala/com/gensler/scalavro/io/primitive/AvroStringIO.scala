package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroString
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.generic.GenericData
import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.apache.avro.Schema

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

object AvroStringIO extends AvroStringIO

trait AvroStringIO extends AvroTypeIO[String] {

  def avroType = AvroString

  protected[scalavro] def asGeneric[S <: String: TypeTag](value: S): String = value

  def write[S <: String: TypeTag](value: S, stream: OutputStream) = {
    val encoder = EncoderFactory.get.binaryEncoder(stream, null)
    encoder writeString value
    encoder.flush
  }

  def read(stream: InputStream) = Try {
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    decoder.readString
  }

  def writeJson[S <: String: TypeTag](value: S, stream: OutputStream) = {
    val encoder = EncoderFactory.get.jsonEncoder(Schema.create(Schema.Type.STRING), stream)
    encoder writeString value
    encoder.flush
  }

  def readJson(stream: InputStream) = ???

}
