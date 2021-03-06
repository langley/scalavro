package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroByte
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ EncoderFactory, DecoderFactory }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

object AvroByteIO extends AvroByteIO

trait AvroByteIO extends AvroTypeIO[Byte] {

  def avroType = AvroByte

  protected[scalavro] def asGeneric[B <: Byte: TypeTag](value: B): Byte = value

  def write[B <: Byte: TypeTag](value: B, stream: OutputStream) = {
    val encoder = EncoderFactory.get.directBinaryEncoder(stream, null)
    encoder writeInt value.toInt
  }

  def read(stream: InputStream) = Try {
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)
    decoder.readInt.toByte
  }

}