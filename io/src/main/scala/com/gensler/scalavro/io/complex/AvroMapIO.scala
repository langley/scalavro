package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.AvroTypeIO.Implicits._
import com.gensler.scalavro.io.primitive.{ AvroLongIO, AvroStringIO }
import com.gensler.scalavro.types.complex.AvroMap
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.util.ReflectionHelpers

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericData, GenericDatumWriter }
import org.apache.avro.io.{ EncoderFactory, DecoderFactory }
import org.apache.avro.util.Utf8

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

case class AvroMapIO[T, M <: Map[String, T]](avroType: AvroMap[T, M]) extends AvroTypeIO[M]()(avroType.originalTypeTag) {

  implicit def itemTypeTag = avroType.itemType.tag
  implicit def originalTypeTag = avroType.originalTypeTag

  val originalTypeVarargsApply = ReflectionHelpers.companionVarargsApply[M] match {
    case Some(methodMirror) => methodMirror
    case None => throw new IllegalArgumentException(
      "Sequence subclasses must have a companion object with a public varargs " +
        "apply method, but no such method was found for type [%s].".format(avroType.originalTypeTag.tpe)
    )
  }

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
  val itemIO = AvroTypeIO.Implicits.avroTypeToIO(avroType.itemType)

  protected[scalavro] def asGeneric[M <: Map[String, T]: TypeTag](map: M): java.util.Map[String, _] =
    scala.collection.JavaConversions mapAsJavaMap map.map { case (key, value) => key -> itemIO.asGeneric(value) }

  def write[M <: Map[String, T]: TypeTag](map: M, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[java.util.Map[String, _]](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(map), encoder)
      encoder.flush
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(map, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val items = new scala.collection.mutable.ArrayBuffer[(String, T)]

    def readBlock(): Long = {
      val numItems = (AvroLongIO read stream).get
      val absNumItems = math abs numItems
      if (numItems < 0L) { val bytesInBlock = (AvroLongIO read stream).get }
      (0L until absNumItems) foreach { _ => items += AvroStringIO.read(stream).get -> avroType.itemType.read(stream).get }
      absNumItems
    }

    var itemsRead = readBlock()
    while (itemsRead != 0L) { itemsRead = readBlock() }
    originalTypeVarargsApply(items).asInstanceOf[M] // a Seq of tuples is passed to varargs MethodMirror.apply
  }

}