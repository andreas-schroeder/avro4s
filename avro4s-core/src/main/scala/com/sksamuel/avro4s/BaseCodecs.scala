package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.time.Instant

import org.apache.avro.generic.{GenericData, GenericFixed}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.JavaConverters._
import scala.collection.generic.CanBuildFrom
import scala.reflect.ClassTag

trait BaseCodecs extends StringCodecs {

  implicit object IntCodec extends Codec[Int] {

    val schema: Schema = SchemaBuilder.builder.intType

    def encode(value: Int): AnyRef = java.lang.Integer.valueOf(value)

    def decode(value: Any): Int = value match {
      case byte: Byte   => byte.toInt
      case short: Short => short.toInt
      case int: Int     => int
      case other        => sys.error(s"Cannot convert $other to type INT")
    }
  }

  implicit object DoubleCodec extends Codec[Double] {

    val schema: Schema = SchemaBuilder.builder.doubleType

    def encode(value: Double): AnyRef = java.lang.Double.valueOf(value)

    def decode(value: Any): Double = value match {
      case d: Double           => d
      case d: java.lang.Double => d
    }
  }

  implicit object BooleanCodec extends Codec[Boolean] {

    val schema: Schema = SchemaBuilder.builder.booleanType

    def encode(value: Boolean): AnyRef = java.lang.Boolean.valueOf(value)

    def decode(value: Any): Boolean = value.asInstanceOf[Boolean]
  }

  implicit object InstantCodec extends Codec[Instant] {
    def schema: Schema = LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType)

    def encode(value: Instant): AnyRef = new java.lang.Long(value.toEpochMilli)

    def decode(value: Any): Instant = value match {
      case long: Long => Instant.ofEpochMilli(long)
      case other      => sys.error(s"Cannot convert $other to type Instant")
    }
  }

  implicit def optionCodec[T](implicit codec: Codec[T]): Codec[Option[T]] = new Codec[Option[T]] {
    val schema: Schema = SchemaBuilder.nullable().`type`(codec.schema)

    def encode(value: Option[T]): AnyRef = value.map(codec.encode).orNull

    def decode(value: Any): Option[T] = if (value == null) None else Option(codec.decode(value))
  }

  sealed trait ByteArrayCodecBase extends Codec[Array[Byte]] with FieldSpecificSchemaTypeCodec[Array[Byte]] {

    def decode(value: Any): Array[Byte] = value match {
      case buffer: ByteBuffer  => buffer.array
      case array: Array[Byte]  => array
      case fixed: GenericFixed => fixed.bytes
    }

    def withFieldSchema(schema: Schema): ByteArrayCodecBase = schema.getType match {
      case Schema.Type.ARRAY => byteArrayCodec
      case Schema.Type.FIXED => new FixedByteArrayCodec(schema)
    }
  }

  implicit object byteArrayCodec extends ByteArrayCodecBase {

    val schema: Schema = SchemaBuilder.builder.bytesType

    def encode(value: Array[Byte]): AnyRef = ByteBuffer.wrap(value)
  }

  class FixedByteArrayCodec(val schema: Schema) extends ByteArrayCodecBase {
    require(schema.getType == Schema.Type.FIXED)

    def encode(value: Array[Byte]): AnyRef = {
      val bb = ByteBuffer.allocate(schema.getFixedSize).put(value)
      GenericData.get.createFixed(null, bb.array(), schema)
    }
  }

  class ByteSeqCodec[T[_]](map: Array[Byte] => T[Byte],
                           comap: T[Byte] => Array[Byte],
                           codec: ByteArrayCodecBase = byteArrayCodec)
      extends Codec[T[Byte]]
      with FieldSpecificSchemaTypeCodec[T[Byte]] {

    val schema = codec.schema

    def encode(value: T[Byte]): AnyRef = codec.encode(comap(value))

    def decode(value: Any): T[Byte] = map(codec.decode(value))

    def withFieldSchema(schema: Schema): Codec[T[Byte]] = new ByteSeqCodec(map, comap, codec.withFieldSchema(schema))
  }

  implicit val byteSeqCodec = new ByteSeqCodec[Seq](_.toSeq, _.toArray)
  implicit val byteListCodec = new ByteSeqCodec[List](_.toList, _.toArray)
  implicit val byteVectorCodec = new ByteSeqCodec[Vector](_.toVector, _.toArray)

  implicit def arrayCodec[T: ClassTag](implicit codec: Codec[T]): Codec[Array[T]] = new Codec[Array[T]] {
    import scala.collection.JavaConverters._

    val schema: Schema = SchemaBuilder.array().items(codec.schema)

    def encode(value: Array[T]): AnyRef = value.map(codec.encode).toList.asJava

    def decode(value: Any): Array[T] = value match {
      case array: Array[_]               => array.map(codec.decode)
      case list: java.util.Collection[_] => list.asScala.map(codec.decode).toArray
      case list: Iterable[_]             => list.map(codec.decode).toArray
      case other                         => sys.error("Unsupported array " + other)
    }
  }

  class IterableCodec[T, C[X] <: Iterable[X]](codec: Codec[T])(
      implicit cbf: CanBuildFrom[Nothing, T, C[T @uncheckedVariance]])
      extends Codec[C[T]] {
    val schema: Schema = SchemaBuilder.array().items(codec.schema)

    def encode(value: C[T]): AnyRef = value.map(codec.encode).toList.asJava

    def decode(value: Any): C[T] = value match {
      case array: Array[_]               => array.map(codec.decode).to[C]
      case list: java.util.Collection[_] => list.asScala.map(codec.decode).to[C]
      case list: Iterable[_]             => list.map(codec.decode).to[C]
      case other                         => sys.error("Unsupported array " + other)
    }
  }

  implicit def seqCodec[T](implicit codec: Codec[T]): Codec[Seq[T]] = new IterableCodec(codec)
  implicit def listCodec[T](implicit codec: Codec[T]): Codec[List[T]] = new IterableCodec(codec)
  implicit def vectorCodec[T](implicit codec: Codec[T]): Codec[Vector[T]] = new IterableCodec(codec)
  implicit def setCodec[T](implicit codec: Codec[T]): Codec[Set[T]] = new IterableCodec(codec)

}