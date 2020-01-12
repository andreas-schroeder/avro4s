package com.sksamuel.avro4s.record

import com.sksamuel.avro4s.record.CodecTest._
import com.sksamuel.avro4s.{AvroDoc, AvroNamespace, Codec, FieldMapper, SnakeCase}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

object CodecTest {

  @AvroNamespace("foo.bar.baz")
  sealed trait Base
  case class Foo(@AvroDoc("Some docs")
                 oneField: String,
                 baz: Baz)
      extends Base

  case class Bar(anotherField: String) extends Base

  case class Baz(fieldA: String, fieldB: String)

  @AvroNamespace("foo.bar.baz")
  sealed trait EnumBase
  case object EnumOne extends EnumBase
  case object EnumTwo extends EnumBase
  case object EnumThree extends EnumBase
}

class CodecTest extends AnyFunSuite with Matchers {

  test("codec should work") {
    implicit val mapper: FieldMapper = SnakeCase
    val codec = Codec[Base]

    println(codec.schema.toString(true))

    val input: Base = CodecTest.Foo("a string", CodecTest.Baz("a", "b"))

    val output: Base = codec.decode(codec.encode(input))

    output shouldBe input
  }

  test("codec should work on enums") {
    implicit val mapper: FieldMapper = SnakeCase
    val codec = Codec[EnumBase]

    println(codec.schema.toString(true))

    val input = EnumThree

    val output = codec.decode(codec.encode(input))

    output shouldBe input
  }

}