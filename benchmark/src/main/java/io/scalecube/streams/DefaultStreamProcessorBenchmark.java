package io.scalecube.streams;

import io.scalecube.transport.Address;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@Fork(2)
@State(Scope.Thread)
@Threads(4)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class DefaultStreamProcessorBenchmark {

  private Address address;
  private DefaultEventStream eventStream;

  @Setup
  public void setUp() {
    address = Address.from("host:0");
    eventStream = new DefaultEventStream();
  }

  @Benchmark
  public void testCreate(Blackhole bh) {
    ChannelContext channelContext = ChannelContext.create(address);
    bh.consume(channelContext);
    // eventStream.subscribe(channelContext);
    // bh.consume(new DefaultStreamProcessor(channelContext));
  }
}
