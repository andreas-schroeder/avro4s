package com.sksamuel.avro4s

/**
  * An [[Encoder]] encodes a Scala value of type T into a compatible
  * Avro value based on the given schema.
  *
  * For example, given a string, and a schema of type Schema.Type.STRING
  * then the string would be encoded as an instance of Utf8, whereas
  * the same string and a Schema.Type.FIXED would be encoded as an
  * instance of GenericFixed.
  *
  * Another example is given a Scala enumeration value, and a schema of
  * type Schema.Type.ENUM, the value would be encoded as an instance
  * of GenericData.EnumSymbol.
  */
trait Encoder[T] extends SchemaAware[Encoder, T] with Serializable { self =>

  /**
   * Encodes the given value to a value supported by Avro's generic data model
   */
  def encode(value: T): AnyRef

  def withSchema(schemaFor: SchemaFor[T]): Encoder[T] = {
    val sf = schemaFor
    new Encoder[T] {
      val schemaFor: SchemaFor[T] = sf

      def encode(value: T): AnyRef = self.encode(value)
    }
  }

}

object Encoder
    extends MagnoliaGeneratedEncoders
    with ShapelessCoproductEncoders
    with ScalaPredefAndCollectionEncoders
    with BigDecimalEncoders
    with ByteIterableEncoders
    with TemporalEncoders
    with BaseEncoders {

  def apply[T](implicit encoder: Encoder[T]): Encoder[T] = encoder

  private class DelegatingEncoder[T, S](encoder: Encoder[T], val schemaFor: SchemaFor[S], comap: S => T) extends Encoder[S] {

    def encode(value: S): AnyRef = encoder.encode(comap(value))

    override def withSchema(schemaFor: SchemaFor[S]): Encoder[S] = {
      // pass through schema so that underlying encoder performs desired transformations.
      val modifiedEncoder = encoder.withSchema(schemaFor.forType)
      new DelegatingEncoder[T, S](modifiedEncoder, schemaFor.forType, comap)
    }
  }

  /**
   * Enables decorating/enhancing an encoder with a transformation function
   */
  implicit class EncoderOps[T](val encoder: Encoder[T]) extends AnyVal {
    def comap[S](f: S => T): Encoder[S] = new DelegatingEncoder(encoder, encoder.schemaFor.forType, f)
  }
}
