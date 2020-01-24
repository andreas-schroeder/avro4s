package com.sksamuel.avro4s

import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import java.util.UUID

import com.sksamuel.avro4s.SchemaUpdate.NoUpdate
import magnolia._
import org.apache.avro.{LogicalType, LogicalTypes, Schema, SchemaBuilder}

import scala.language.experimental.macros
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * A [[SchemaFor]] generates an Avro Schema for a Scala or Java type.
  *
  * For example, a String SchemaFor could return an instance of Schema.Type.STRING
  * or Schema.Type.FIXED depending on the type required for Strings.
  */
final case class SchemaForV2[T](schema: Schema, fieldMapper: FieldMapper = DefaultFieldMapper) extends Serializable {

  /**
    * Creates a SchemaFor[U] by applying a function Schema => Schema
    * to the schema generated by this instance.
    */
  def map[U](fn: Schema => Schema): SchemaForV2[U] = SchemaForV2[U](fn(schema), fieldMapper)

  def forType[U]: SchemaForV2[U] = map[U](identity)
}

object SchemaForV2 {

  def apply[T](implicit schemaFor: SchemaForV2[T]): SchemaForV2[T] = schemaFor

  implicit def gen[T]: Typeclass[T] = macro Magnolia.gen[T]

  type Typeclass[T] = SchemaForV2[T]

  def dispatch[T: WeakTypeTag](ctx: SealedTrait[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): SchemaForV2[T] =
    DatatypeShape.of(ctx) match {
      case SealedTraitShape.TypeUnion => TypeUnions.schema(ctx, NoUpdate, fieldMapper)
      case SealedTraitShape.ScalaEnum => SchemaForV2[T](ScalaEnums.schema(ctx), fieldMapper)
    }

  def combine[T](ctx: CaseClass[Typeclass, T])(
      implicit fieldMapper: FieldMapper = DefaultFieldMapper): SchemaForV2[T] = {
    val paramSchema = (p: Param[Typeclass, T]) => p.typeclass.schema

    DatatypeShape.of(ctx) match {
      case CaseClassShape.Record => Records.buildSchema(ctx, fieldMapper, None, paramSchema)
      case CaseClassShape.ValueType =>
        SchemaForV2[T](ValueTypes.buildSchema(ctx, None, paramSchema), fieldMapper)
    }
  }

  implicit val IntSchema: SchemaForV2[Int] = SchemaForV2[Int](SchemaBuilder.builder.intType)
  implicit val ByteSchema: SchemaForV2[Byte] = IntSchema.forType
  implicit val ShortSchema: SchemaForV2[Short] = IntSchema.forType
  implicit val LongSchema: SchemaForV2[Long] = IntSchema.forType
  implicit val FloatSchema: SchemaForV2[Float] = SchemaForV2[Float](SchemaBuilder.builder.floatType)
  implicit val DoubleSchema: SchemaForV2[Double] = SchemaForV2[Double](SchemaBuilder.builder.doubleType)
  implicit val BooleanSchema: SchemaForV2[Boolean] = SchemaForV2[Boolean](SchemaBuilder.builder.booleanType)
  implicit val ByteBufferSchema: SchemaForV2[ByteBuffer] = SchemaForV2[ByteBuffer](SchemaBuilder.builder.bytesType)
  implicit val CharSequenceSchema: SchemaForV2[CharSequence] =
    SchemaForV2[CharSequence](SchemaBuilder.builder.stringType)
  implicit val StringSchema: SchemaForV2[String] = SchemaForV2[String](SchemaBuilder.builder.stringType)
  implicit val UUIDSchema: SchemaForV2[UUID] = StringSchema.forType

  implicit def javaEnumSchema[E <: Enum[_]](implicit tag: ClassTag[E]): SchemaForV2[E] = {
    val typeInfo = TypeInfo.fromClass(tag.runtimeClass)
    val nameExtractor = NameExtractor(typeInfo)
    val symbols = tag.runtimeClass.getEnumConstants.map(_.toString)
    val schema = SchemaBuilder.enumeration(nameExtractor.name).namespace(nameExtractor.namespace).symbols(symbols: _*)
    SchemaForV2[E](schema)
  }

  implicit def scalaEnumSchema[E <: scala.Enumeration#Value](implicit tag: TypeTag[E]): SchemaForV2[E] = {
    val typeRef = tag.tpe match {
      case t@TypeRef(_, _, _) => t
    }

    val valueType = typeOf[E]
    val pre = typeRef.pre.typeSymbol.typeSignature.members.sorted
    val syms = pre.filter { sym =>
      !sym.isMethod &&
        !sym.isType &&
        sym.typeSignature.baseType(valueType.typeSymbol) =:= valueType
    }.map { sym =>
      sym.name.decodedName.toString.trim
    }

    val as = typeRef.pre.typeSymbol.annotations
    val nameAnnotation = as.collectFirst {
      case a: AvroName => a.name
    }
    val namespaceAnnotation = as.collectFirst {
      case a: AvroNamespace => a.namespace
    }
    val props = as.collect {
      case prop: AvroProp => prop.key -> prop.value
    }

    val nameExtractor = NameExtractor(TypeInfo.fromType(typeRef.pre))

    val s = SchemaBuilder.enumeration(nameExtractor.name).namespace(nameExtractor.namespace).symbols(syms: _*)
    props.foreach { case (key, value) =>
      s.addProp(key, value)
    }
    SchemaForV2[E](s)
  }

  object TimestampNanosLogicalType extends LogicalType("timestamp-nanos") {
    override def validate(schema: Schema): Unit = {
      super.validate(schema)
      if (schema.getType != Schema.Type.LONG) {
        throw new IllegalArgumentException("Logical type timestamp-nanos must be backed by long")
      }
    }
  }

  implicit val InstantSchema: SchemaForV2[Instant] =
    SchemaForV2[Instant](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))
  implicit val DateSchema: SchemaForV2[Date] = SchemaForV2(
    LogicalTypes.date().addToSchema(SchemaBuilder.builder.intType))
  implicit val LocalDateSchema: SchemaForV2[LocalDate] = DateSchema.forType
  implicit val LocalDateTimeSchema: SchemaForV2[LocalDateTime] = SchemaForV2(
    TimestampNanosLogicalType.addToSchema(SchemaBuilder.builder.longType))
  implicit val LocalTimeSchema: SchemaForV2[LocalTime] = SchemaForV2(
    LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder.longType()))
  implicit val TimestampSchema: SchemaForV2[Timestamp] =
    SchemaForV2[Timestamp](LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder.longType))

  implicit def optionSchema[T](schemaForItem: SchemaForV2[T]): SchemaForV2[Option[T]] =
    schemaForItem.map[Option[T]](itemSchema =>
      SchemaHelper.createSafeUnion(itemSchema, SchemaBuilder.builder().nullType()))

  implicit def eitherSchema[A, B](implicit leftFor: SchemaForV2[A],
                                  rightFor: SchemaForV2[B]): SchemaForV2[Either[A, B]] =
    SchemaForV2[Either[A, B]](SchemaHelper.createSafeUnion(leftFor.schema, rightFor.schema))

  implicit def byteIterableSchema[C[X] <: Iterable[X]]: SchemaForV2[C[Byte]] =
    SchemaForV2[C[Byte]](SchemaBuilder.builder.bytesType)

  implicit def iterableSchema[C[X] <: Iterable[X], T](implicit schemaForItem: SchemaForV2[T]): SchemaForV2[C[T]] =
    SchemaForV2[C[T]](SchemaBuilder.array.items(schemaForItem.schema))

  implicit def arraySchema[T](implicit schemaForItem: SchemaForV2[T]): SchemaForV2[Array[T]] =
    iterableSchema(schemaForItem).forType

  implicit def bigDecimalSchema(implicit sp: ScalePrecision = ScalePrecision.default): SchemaForV2[BigDecimal] =
    SchemaForV2(LogicalTypes.decimal(sp.precision, sp.scale).addToSchema(SchemaBuilder.builder.bytesType))

}
