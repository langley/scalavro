package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroBoolean
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.apache.avro.Schema

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

object AvroBooleanIO extends AvroBooleanIO

trait AvroBooleanIO extends AvroTypeIO[Boolean] {

  def avroType = AvroBoolean

  final val trueByte = 1.toByte
  final val falseByte = 0.toByte

  protected[scalavro] def asGeneric[B <: Boolean: TypeTag](value: B): Boolean = value

  /**
    * a boolean is written as a single byte whose value is either 0 (false) or
    * 1 (true).
    */
  def write[B <: Boolean: TypeTag](value: B, stream: OutputStream) =
    stream.write { if (value) trueByte else falseByte }

  def read(stream: InputStream) = Try {
    stream.read match {
      case `trueByte`  => true
      case `falseByte` => false
      case _           => throw new AvroDeserializationException[Boolean]
    }
  }

  def writeJson[B <: Boolean: TypeTag](value: B, stream: OutputStream) = {
    val encoder = EncoderFactory.get.jsonEncoder(Schema.create(Schema.Type.NULL), stream)
    encoder writeBoolean value
    encoder.flush
  }

  def readJson(stream: InputStream) = ???

}