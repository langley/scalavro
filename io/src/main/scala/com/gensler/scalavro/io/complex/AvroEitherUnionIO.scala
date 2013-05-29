package com.gensler.scalavro.io.complex

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.io.primitive.AvroLongIO
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.types.complex.AvroUnion
import com.gensler.scalavro.error.{ AvroSerializationException, AvroDeserializationException }
import com.gensler.scalavro.io.AvroTypeIO.Implicits._
import com.gensler.scalavro.util.Union
import com.gensler.scalavro.util.Union._

import scala.util.{ Try, Success, Failure }
import scala.reflect.runtime.universe._

import java.io.{ InputStream, OutputStream }

private[scalavro] case class AvroEitherUnionIO[U <: Union.not[_]: TypeTag, T <: Either[_, _]: TypeTag](
    avroType: AvroUnion[U, T]) extends AvroUnionIO[U, T] {

  val TypeRef(_, _, List(leftType, rightType)) = typeOf[T]

  val leftAvroType = avroType.memberAvroTypes.find { at => leftType <:< at.tag.tpe }.get
  val rightAvroType = avroType.memberAvroTypes.find { at => rightType <:< at.tag.tpe }.get

  protected[scalavro] def asGeneric[X <: T: TypeTag](obj: X) =
    asGenericHelper(obj)(typeTag[X], leftAvroType.tag, rightAvroType.tag)

  private def asGenericHelper[X <: T: TypeTag, A: TypeTag, B: TypeTag](obj: X) = obj match {
    case Left(value)  => leftAvroType.asInstanceOf[AvroType[A]].asGeneric(value.asInstanceOf[A])
    case Right(value) => rightAvroType.asInstanceOf[AvroType[B]].asGeneric(value.asInstanceOf[B])
  }

  def write[X <: T: TypeTag](obj: X, stream: OutputStream) = {
    AvroLongIO.write(if (obj.isLeft) 0L else 1L, stream)
    writeHelper(obj, stream)(typeTag[X], leftAvroType.tag, rightAvroType.tag)
  }

  def writeHelper[X <: T: TypeTag, A: TypeTag, B: TypeTag](obj: X, stream: OutputStream) =
    obj match {
      case Left(value)  => leftAvroType.asInstanceOf[AvroType[A]].write(value.asInstanceOf[A], stream)
      case Right(value) => rightAvroType.asInstanceOf[AvroType[B]].write(value.asInstanceOf[B], stream)
    }

  def read(stream: InputStream) = Try {
    readHelper(stream)(leftAvroType.tag, rightAvroType.tag).asInstanceOf[T]
  }

  def readHelper[A: TypeTag, B: TypeTag](stream: InputStream) = {
    val index = AvroLongIO.read(stream).get
    if (index == 0) Left(leftAvroType.read(stream).get.asInstanceOf[A])
    else if (index == 1) Right(rightAvroType.read(stream).get.asInstanceOf[B])
    else throw new AvroDeserializationException[T]
  }

  def writeJson[X <: T: TypeTag](obj: X, stream: OutputStream) = ???

  def readJson(stream: InputStream) = ???

}