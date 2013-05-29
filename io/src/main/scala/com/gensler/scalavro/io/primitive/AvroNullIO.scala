package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroNull
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.generic.GenericData
import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.apache.avro.Schema

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

object AvroNullIO extends AvroNullIO

trait AvroNullIO extends AvroTypeIO[Unit] {

  def avroType = AvroNull

  protected[scalavro] def asGeneric[U <: Unit: TypeTag](value: U): Unit = Unit

  // null is written as zero bytes.
  def write[U <: Unit: TypeTag](value: U, stream: OutputStream) {}

  def read(stream: InputStream) = Success(())

  def writeJson[U <: Unit: TypeTag](value: U, stream: OutputStream) = {
    val encoder = EncoderFactory.get.jsonEncoder(Schema.create(Schema.Type.NULL), stream)
    encoder.writeNull
    encoder.flush
  }

  def readJson(stream: InputStream) = ???

}