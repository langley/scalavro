package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroDouble
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.apache.avro.Schema

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

object AvroDoubleIO extends AvroDoubleIO

trait AvroDoubleIO extends AvroTypeIO[Double] {

  def avroType = AvroDouble

  protected[scalavro] def asGeneric[D <: Double: TypeTag](value: D): Double = value

  def write[D <: Double: TypeTag](value: D, stream: OutputStream) = {
    val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)
    encoder writeDouble value
  }

  def read(stream: InputStream) = Try {
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    decoder.readDouble
  }

  def writeJson[D <: Double: TypeTag](value: D, stream: OutputStream) = {
    val encoder = EncoderFactory.get.jsonEncoder(Schema.create(Schema.Type.DOUBLE), stream)
    encoder writeDouble value
    encoder.flush
  }

  def readJson(stream: InputStream) = ???

}