package com.sksamuel.avro4s

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

import scala.util.Try

final case class AvroJsonInputStream[T](in: InputStream,
                                        writerSchema: Schema)
                                       (implicit decoder: Decoder[T]) extends AvroInputStream[T] {

  val resolved = decoder.resolveDecoder()

  private val datumReader = new DefaultAwareDatumReader[GenericRecord](writerSchema, resolved.schema)
  private val jsonDecoder = DecoderFactory.get.jsonDecoder(writerSchema, in)

  private var record: GenericRecord = null
  private def next = Try {
    record = datumReader.read(record, jsonDecoder)
    record
  }

  def iterator: Iterator[T] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(resolved.decode(_))

  def tryIterator: Iterator[Try[T]] = Iterator.continually(next)
    .takeWhile(_.isSuccess)
    .map(_.get)
    .map(record => Try(resolved.decode(record)))

  def singleEntity: Try[T] = next.map(resolved.decode(_))

  override def close(): Unit = in.close()
}
