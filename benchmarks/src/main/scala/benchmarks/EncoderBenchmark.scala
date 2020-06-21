package benchmarks

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import benchmarks.record._
import com.sksamuel.avro4s._
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

object EncoderBenchmark extends BenchmarkHelpers {

  @State(Scope.Thread)
  class Setup {
    val record = RecordWithUnionAndTypeField(AttributeValue.Valid[Int](255, t))

    val specificRecord = {
      import benchmarks.record.generated.AttributeValue._
      import benchmarks.record.generated._
      new RecordWithUnionAndTypeField(new ValidInt(255, t))
    }

    val (avro4sEncoder, avro4sWriter) = {
      val schema = AvroSchema[RecordWithUnionAndTypeField]
      val encoder = Encoder[RecordWithUnionAndTypeField]
      val writer = new GenericDatumWriter[GenericRecord](schema)
      (encoder, writer)
    }

    val (handrolledEncoder, handrolledWriter) = {
      import benchmarks.handrolled_codecs._
      implicit val codec: AttributeValueCodec[Int] = AttributeValueCodec[Int]
      implicit val schemaForValid = codec.schemaForValid
      val schema = AvroSchema[RecordWithUnionAndTypeField]
      val encoder = Encoder[RecordWithUnionAndTypeField]
      val writer = new GenericDatumWriter[GenericRecord](schema)
      (encoder, writer)
    }

  }
}

class EncoderBenchmark extends CommonParams with BenchmarkHelpers {

  import EncoderBenchmark._

  def encode[T](value: T, encoder: Encoder[T], writer: GenericDatumWriter[GenericRecord]): ByteBuffer = {
    val outputStream = new ByteArrayOutputStream(512)
    val record = encoder.encode(value).asInstanceOf[GenericRecord]
    val enc = EncoderFactory.get().directBinaryEncoder(outputStream, null)
    writer.write(record, enc)
    ByteBuffer.wrap(outputStream.toByteArray)
  }


  @Benchmark
  def avroSpecificRecord(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(setup.specificRecord.toByteBuffer)

  @Benchmark
  def avro4sGenerated(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(encode(setup.record, setup.avro4sEncoder, setup.avro4sWriter))

  @Benchmark
  def avro4sHandrolled(setup: Setup, blackhole: Blackhole) =
    blackhole.consume(encode(setup.record, setup.handrolledEncoder, setup.handrolledWriter))
}
