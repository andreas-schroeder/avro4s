package benchmarks

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import benchmarks.InputStreamsBenchmark.Setup
import benchmarks.record.{AttributeValue, RecordWithUnionAndTypeField}
import com.sksamuel.avro4s.{AvroDataInputStream, AvroDataOutputStream}
import org.apache.avro.file.CodecFactory
import org.openjdk.jmh.annotations.{Benchmark, Scope, State}
import org.openjdk.jmh.infra.Blackhole

object InputStreamsBenchmark extends BenchmarkHelpers {

  @State(Scope.Thread)
  class Setup {
    private val outputStream = new ByteArrayOutputStream()
    private val dataOutputStream = new AvroDataOutputStream[RecordWithUnionAndTypeField](outputStream, CodecFactory.nullCodec())
    dataOutputStream.write(0.to(1000).map(i => RecordWithUnionAndTypeField(AttributeValue.Valid[Int](i, t))))
    dataOutputStream.close()
    private val bytes = outputStream.toByteArray

    def avroDataStream =
      new AvroDataInputStream[RecordWithUnionAndTypeField](new ByteArrayInputStream(bytes), None)
  }
}

class InputStreamsBenchmark extends CommonParams with BenchmarkHelpers {

  @Benchmark
  def avroDataInputStream(setup: Setup, blackhole: Blackhole) = {
    val it = setup.avroDataStream.iterator
    while(it.hasNext) {
      blackhole.consume(it.next)
    }
  }
}
