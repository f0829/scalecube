package io.scalecube.streams;

import static org.junit.Assert.assertNotSame;

import io.reactivex.observers.TestObserver;
import io.reactivex.subscribers.TestSubscriber;
import io.scalecube.transport.Address;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class StreamProcessorsTest {

  private static final int CONNECT_TIMEOUT_MILLIS = (int) Duration.ofMillis(1000).toMillis();
  private static final long TIMEOUT_MILLIS = Duration.ofMillis(3000).toMillis();

  private Bootstrap bootstrap;

  @Before
  public void setUp() {
    bootstrap = new Bootstrap()
        .group(new NioEventLoopGroup(0))
        .channel(NioSocketChannel.class)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true);
  }

  @After
  public void cleanUp() {
    bootstrap.group().shutdownGracefully();
  }

  private TestSubscriber<StreamMessage> trySend(ClientStreamProcessors clientStreamProcessors) {
    StreamProcessor streamProcessor = clientStreamProcessors.create(Address.from("localhost:0"));
    TestSubscriber<StreamMessage> subscriber = streamProcessor.listen().test();
    streamProcessor.onNext(StreamMessage.builder().qualifier("q/test").build());
    return subscriber;
  }

  @Test
  public void testClientStreamProcessorsNotSameAfterSetup() {
    ClientStreamProcessors defaultOne = StreamProcessors.newClient();

    ClientStreamProcessors first = defaultOne.bootstrap(new Bootstrap()); // client with bootstrap
    ClientStreamProcessors second = defaultOne.bootstrap(new Bootstrap()); // another client with custom bootstrap

    assertNotSame(defaultOne, first);
    assertNotSame(defaultOne, second);
    assertNotSame(first, second);
  }

  @Test
  public void testClientStreamProcessorsHaveSeparateClientStreams() {
    ClientStreamProcessors defaultOne = StreamProcessors.newClient();

    // branching default => custom instance (custom is sense it has another connection timeout)
    ClientStreamProcessors customOne = defaultOne.bootstrap(bootstrap);

    // connect and send msgs and fail withing 1 second (by default connect timout is much longer than 1 second)
    TestSubscriber<StreamMessage> streamMessageTestSubscriber = trySend(customOne);
    Assert.assertTrue(streamMessageTestSubscriber.awaitTerminalEvent(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
    streamMessageTestSubscriber.assertNotComplete()
        .assertNoValues()
        .assertError(ConnectException.class);
  }
}
