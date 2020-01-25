package com.sksamuel.avro4s.record.encoder

import java.util.UUID

import com.sksamuel.avro4s.{AvroSchemaV2, EncoderV2, ImmutableRecord}
import org.apache.avro.util.Utf8
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UUIDEncoderTest extends AnyWordSpec with Matchers {

  import scala.collection.JavaConverters._

  "Encoder" should {
    "encode uuids" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchemaV2[UUIDTest]
      EncoderV2[UUIDTest].encode(UUIDTest(uuid)) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
    }
    "encode seq of uuids" in {
      val uuid1 = UUID.randomUUID()
      val uuid2 = UUID.randomUUID()
      val schema = AvroSchemaV2[UUIDSeq]
      EncoderV2[UUIDSeq].encode(UUIDSeq(Seq(uuid1, uuid2))) shouldBe ImmutableRecord(schema, Vector(List(new Utf8(uuid1.toString), new Utf8(uuid2.toString)).asJava))
    }
    "encode UUIDs with defaults" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchemaV2[UUIDDefault]
      EncoderV2[UUIDDefault].encode(UUIDDefault(uuid)) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
    }
    "encode Option[UUID]" in {
      val uuid = UUID.randomUUID()
      val schema = AvroSchemaV2[UUIDOption]
      EncoderV2[UUIDOption].encode(UUIDOption(Some(uuid))) shouldBe ImmutableRecord(schema, Vector(new Utf8(uuid.toString)))
      EncoderV2[UUIDOption].encode(UUIDOption(None)) shouldBe ImmutableRecord(schema, Vector(null))
    }
  }
}

case class UUIDTest(uuid: UUID)
case class UUIDSeq(uuids: Seq[UUID])
case class UUIDDefault(uuid: UUID = UUID.fromString("86da265c-95bd-443c-8860-9381efca059d"))
case class UUIDOption(uuid: Option[UUID])