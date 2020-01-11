package com.sksamuel.avro4s

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericData}
import org.apache.avro.util.Utf8
import shapeless.{:+:, CNil, Coproduct, Inl, Inr}

import scala.reflect.runtime.universe._
import scala.util.Try

class CoproductBaseCodec[S: WeakTypeTag: Manifest](codec: Codec[S]) extends Codec[S :+: CNil] {

  import scala.collection.JavaConverters._

  val schema: Schema = {
    val base = codec.schema
    val schemas = Try(base.getTypes.asScala).getOrElse(Seq(base))
    Schema.createUnion(schemas.asJava)
  }

  def encode(value: S :+: CNil): AnyRef = value match {
    case Inl(h) => codec.encode(h)
    case x      => sys.error(s"Unexpected value '$x' of type CNil (that doesn't exist)")
  }

  private val elementDecoder = ShapelessCoproductCodec.buildElementDecoder(codec)

  def decode(value: Any): S :+: CNil =
    if (elementDecoder.isDefinedAt(value)) Inl(elementDecoder(value))
    else sys.error(s"Unable to decode value '$value'")
}

class CoproductCodec[H: WeakTypeTag: Manifest, T <: Coproduct](codecH: Codec[H], codecT: Codec[T])
    extends Codec[H :+: T] {
  val schema: Schema = SchemaHelper.createSafeUnion(codecH.schema, codecT.schema)

  def encode(value: H :+: T): AnyRef = value match {
    case Inl(h) => codecH.encode(h)
    case Inr(t) => codecT.encode(t)
  }

  private val elementDecoder = ShapelessCoproductCodec.buildElementDecoder(codecH)

  def decode(value: Any) =
    if (elementDecoder.isDefinedAt(value)) Inl(elementDecoder(value))
    else Inr(codecT.decode(value))
}

object ShapelessCoproductCodec {

  def buildElementDecoder[T: WeakTypeTag: Manifest](codec: Codec[T]): PartialFunction[Any, T] = {
    import scala.reflect.runtime.universe.typeOf

    val tpe = implicitly[WeakTypeTag[T]].tpe

    if (tpe <:< typeOf[java.lang.String]) stringDecoder(codec)
    else if (tpe <:< typeOf[Boolean]) booleanDecoder(codec)
    else if (tpe <:< typeOf[Int]) intDecoder(codec)
    else if (tpe <:< typeOf[Long]) longDecoder(codec)
    else if (tpe <:< typeOf[Double]) doubleDecoder(codec)
    else if (tpe <:< typeOf[Float]) floatDecoder(codec)
    else if (tpe <:< typeOf[Array[_]] || tpe <:< typeOf[java.util.Collection[_]] || tpe <:< typeOf[Iterable[_]]) {
      arrayDecoder(codec)
    } else if (tpe <:< typeOf[java.util.Map[_, _]] || tpe <:< typeOf[Map[_, _]]) {
      mapDecoder(codec)
    } else {
      val nameExtractor = NameExtractor(manifest.runtimeClass)
      recordDecoder(nameExtractor.fullName, codec)
    }
  }

  def stringDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Utf8   => codec.decode(v)
    case v: String => codec.decode(v)
  }

  def booleanDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Boolean => codec.decode(v)
  }

  def intDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Int => codec.decode(v)
  }

  def longDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Long => codec.decode(v)
  }

  def doubleDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Double => codec.decode(v)
  }

  def floatDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: Float => codec.decode(v)
  }

  def arrayDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: GenericData.Array[_] => codec.decode(v)
  }

  def mapDecoder[T](codec: Codec[T]): PartialFunction[Any, T] = {
    case v: java.util.Map[_, _] => codec.decode(v)
  }

  def recordDecoder[T](typeName: String, codec: Codec[T]): PartialFunction[Any, T] = {
    case v: GenericContainer if v.getSchema.getFullName == typeName => codec.decode(v)
  }
}
