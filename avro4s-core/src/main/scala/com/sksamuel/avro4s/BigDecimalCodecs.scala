package com.sksamuel.avro4s

import java.nio.ByteBuffer

import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.generic.GenericFixed
import org.apache.avro.{Conversions, LogicalTypes, Schema, SchemaBuilder}

import scala.math.BigDecimal.RoundingMode
import scala.math.BigDecimal.RoundingMode.RoundingMode

trait BigDecimalCodecs {

  implicit def bigDecimalCodec(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
                               roundingMode: RoundingMode = RoundingMode.UNNECESSARY): Codec[BigDecimal] = {
    val decimal = LogicalTypes.decimal(scalePrecision.precision, scalePrecision.scale)
    new BigDecimalsV2.BigDecimalBytesCodec(decimal.addToSchema(SchemaBuilder.builder.bytesType), roundingMode)
  }
}

trait BigDecimalDecoders {

  implicit def bigDecimalDecoder(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
                               roundingMode: RoundingMode = RoundingMode.UNNECESSARY): DecoderV2[BigDecimal] = {
    val decimal = LogicalTypes.decimal(scalePrecision.precision, scalePrecision.scale)
    new BigDecimalsV2.BigDecimalBytesCodec(decimal.addToSchema(SchemaBuilder.builder.bytesType), roundingMode)
  }
}

trait BigDecimalEncoders {

  implicit def bigDecimalEncoder(implicit scalePrecision: ScalePrecision = ScalePrecision.default,
                                 roundingMode: RoundingMode = RoundingMode.UNNECESSARY): EncoderV2[BigDecimal] = {
    val decimal = LogicalTypes.decimal(scalePrecision.precision, scalePrecision.scale)
    new BigDecimalsV2.BigDecimalBytesCodec(decimal.addToSchema(SchemaBuilder.builder.bytesType), roundingMode)
  }
}


object BigDecimalsV2 {

  abstract class BigDecimalCodecBase(roundingMode: RoundingMode) extends Codec[BigDecimal] {

    def encoderUtils = {
      val decimal = schema.getLogicalType.asInstanceOf[Decimal]
      val converter = new Conversions.DecimalConversion
      val rm = java.math.RoundingMode.valueOf(roundingMode.id)
      (decimal, converter, rm)
    }

    override def withSchema(schemaFor: SchemaForV2[BigDecimal]): Codec[BigDecimal] = {
      val schema = schemaFor.schema
      schema.getType match {
        case Schema.Type.BYTES  => new BigDecimalBytesCodec(schema, roundingMode)
        case Schema.Type.STRING => new BigDecimalStringCodec(schema, roundingMode)
        case Schema.Type.FIXED  => new BigDecimalFixedCodec(schema, roundingMode)
        case t =>
          sys.error(s"Unable to create codec with schema type $t, only bytes, fixed, and string supported")
      }
    }
  }

  class BigDecimalBytesCodec(val schema: Schema, roundingMode: RoundingMode) extends BigDecimalCodecBase(roundingMode) {

    val (decimal, converter, rm) = encoderUtils

    def encode(value: BigDecimal): AnyRef =
      converter.toBytes(value.underlying.setScale(decimal.getScale, rm), schema, decimal)

    def decode(value: Any): BigDecimal = value match {
      case bb: ByteBuffer => converter.fromBytes(bb, schema, decimal)
      case _              => sys.error(s"Unable to decode '$value' to BigDecimal via ByteBuffer")
    }
  }

  class BigDecimalStringCodec(val schema: Schema, roundingMode: RoundingMode)
    extends BigDecimalCodecBase(roundingMode) {
    def encode(value: BigDecimal): AnyRef = BaseTypes.StringCodec.encode(value.toString())

    def decode(value: Any): BigDecimal = BigDecimal(BaseTypes.StringCodec.decode(value))
  }

  class BigDecimalFixedCodec(val schema: Schema, roundingMode: RoundingMode) extends BigDecimalCodecBase(roundingMode) {
    val (decimal, converter, rm) = encoderUtils

    def encode(value: BigDecimal): AnyRef =
      converter.toFixed(value.underlying.setScale(decimal.getScale, rm), schema, decimal)

    def decode(value: Any): BigDecimal = value match {
      case f: GenericFixed => converter.fromFixed(f, schema, decimal)
      case _               => sys.error(s"Unable to decode $value to BigDecimal via GenericFixed")
    }
  }

}