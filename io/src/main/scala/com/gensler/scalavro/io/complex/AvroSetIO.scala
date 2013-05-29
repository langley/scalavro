package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.AvroTypeIO.Implicits._
import com.gensler.scalavro.io.primitive.AvroLongIO
import com.gensler.scalavro.types.complex.AvroSet
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{ GenericData, GenericArray, GenericDatumWriter }
import org.apache.avro.io.EncoderFactory

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe.TypeTag

import java.io.{ InputStream, OutputStream }

case class AvroSetIO[T](avroType: AvroSet[T]) extends AvroTypeIO[Set[T]]()(avroType.tag) {

  implicit def itemTypeTag = avroType.itemType.tag

  protected lazy val avroSchema: Schema = (new Parser) parse avroType.selfContainedSchema().toString
  val itemIO = AvroTypeIO.Implicits.avroTypeToIO(avroType.itemType)

  protected[scalavro] def asGeneric[G <: Set[T]: TypeTag](items: G): GenericArray[Any] = {
    import scala.collection.JavaConversions.seqAsJavaList
    val genericArray = new GenericData.Array[Any](items.size, avroSchema)
    genericArray addAll seqAsJavaList(items.toSeq map { itemIO.asGeneric })
    genericArray
  }

  def write[G <: Set[T]: TypeTag](obj: G, stream: OutputStream) = {
    try {
      val datumWriter = new GenericDatumWriter[GenericArray[_]](avroSchema)
      val encoder = EncoderFactory.get.binaryEncoder(stream, null)
      datumWriter.write(asGeneric(obj), encoder)
      encoder.flush
    }
    catch {
      case cause: Throwable =>
        throw new AvroSerializationException(obj, cause)
    }
  }

  def read(stream: InputStream) = Try {
    val items = new scala.collection.mutable.ArrayBuffer[T]

    def readBlock(): Long = {
      val numItems = (AvroLongIO read stream).get
      val absNumItems = math abs numItems
      if (numItems < 0L) { val bytesInBlock = (AvroLongIO read stream).get }
      (0L until absNumItems) foreach { _ => items += avroType.itemType.read(stream).get }
      absNumItems
    }

    var itemsRead = readBlock()
    while (itemsRead != 0L) { itemsRead = readBlock() }
    items.toSet
  }

  def writeJson[G <: Set[T]: TypeTag](obj: G, stream: OutputStream) = ???

  def readJson(stream: InputStream) = ???

}