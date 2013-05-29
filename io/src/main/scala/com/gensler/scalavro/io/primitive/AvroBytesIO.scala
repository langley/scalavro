package com.gensler.scalavro.io.primitive

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.primitive.AvroBytes
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.io.{ EncoderFactory, DecoderFactory }

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }
import java.nio.ByteBuffer

object AvroBytesIO extends AvroBytesIO

trait AvroBytesIO extends AvroTypeIO[Seq[Byte]] {

  def avroType = AvroBytes

  protected[scalavro] def asGeneric[B <: Seq[Byte]: TypeTag](value: B): ByteBuffer = ByteBuffer.wrap(value.toArray)

  def write[B <: Seq[Byte]: TypeTag](bytes: B, stream: OutputStream) = {
    AvroLongIO.write(bytes.length.toLong, stream)
    stream write bytes.toArray
  }

  def read(stream: InputStream) = Try {
    val length = AvroLongIO.read(stream).get
    val buffer = Array.ofDim[Byte](length.toInt)
    val decoder = DecoderFactory.get.directBinaryDecoder(stream, null)

    try {
      decoder.readFixed(buffer)
      buffer.toIndexedSeq
    }
    catch { case cause: Throwable => throw new AvroDeserializationException[Seq[Byte]](cause) }
  }

  def writeJson[B <: Seq[Byte]: TypeTag](value: B, stream: OutputStream) = ???

  def readJson(stream: InputStream) = ???

}