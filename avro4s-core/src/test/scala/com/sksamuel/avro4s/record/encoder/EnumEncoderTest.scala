package com.sksamuel.avro4s.record.encoder

import com.sksamuel.avro4s.schema.{Colours, CupcatEnum, SnoutleyEnum, Wine}
import com.sksamuel.avro4s.{AvroSchemaV2, EncoderV2, ImmutableRecord}
import org.apache.avro.generic.GenericData.EnumSymbol
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EnumEncoderTest extends AnyWordSpec with Matchers {

  "Encoder" should {
    "encode java enums" in {
      case class Test(wine: Wine)
      val schema = AvroSchemaV2[Test]
      val expected = ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      val actual = EncoderV2[Test].encode(Test(Wine.Malbec))
      actual shouldBe expected
    }
    "support optional java enums" in {
      case class Test(wine: Option[Wine])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Some(Wine.Malbec))) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("wine").schema(), "Malbec")))
      EncoderV2[Test].encode(Test(None)) shouldBe ImmutableRecord(schema, Vector(null))
    }
    "support scala enums" in {
      case class Test(value: Colours.Value)
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Colours.Amber)) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Amber")))
    }
    "support optional scala enums" in {
      case class Test(value: Option[Colours.Value])
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test(Some(Colours.Green))) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Green")))
    }
    "support scala enums with defaults" in {
      case class Test(value: Colours.Value = Colours.Red)
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test()) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "Red")))
    }
    "support sealed trait enums with defaults" in {
      case class Test(value: CupcatEnum = SnoutleyEnum)
      val schema = AvroSchemaV2[Test]
      EncoderV2[Test].encode(Test()) shouldBe ImmutableRecord(schema, Vector(new EnumSymbol(schema.getField("value").schema(), "SnoutleyEnum")))
    }
  }
}

