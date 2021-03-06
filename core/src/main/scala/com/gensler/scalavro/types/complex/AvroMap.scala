package com.gensler.scalavro.types.complex

import com.gensler.scalavro.types.{ AvroType, AvroComplexType }
import com.gensler.scalavro.types.primitive.AvroNull
import com.gensler.scalavro.JsonSchemaProtocol._

import spray.json._

import scala.reflect.runtime.universe._
import scala.collection.immutable.ListMap
import scala.util.Success

class AvroMap[T, M <: Map[String, T]](
  implicit val itemTypeTag: TypeTag[T],
  implicit val originalTypeTag: TypeTag[M])
    extends AvroComplexType[M] {

  val itemType = AvroType.fromType[T].get

  val typeName = "map"

  def schema() = new JsObject(ListMap(
    "type" -> typeName.toJson,
    "values" -> itemType.schema
  ))

  def selfContainedSchema(
    resolvedSymbols: scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()) = new JsObject(ListMap(
    "type" -> typeName.toJson,
    "values" -> selfContainedSchemaOrFullyQualifiedName(itemType, resolvedSymbols)
  ))

  override def parsingCanonicalForm(): JsValue = new JsObject(ListMap(
    "type" -> typeName.toJson,
    "values" -> itemType.canonicalFormOrFullyQualifiedName
  ))

}
