package io.scalecube.streams;

import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.subscribers.TestSubscriber;
import io.scalecube.streams.codec.StreamMessageDataCodecImpl;
import io.scalecube.streams.codec.StreamMessageDataCodec;
import io.scalecube.transport.Address;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rx.observers.TestObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.reactivex.observers.BaseTestConsumer.TestWaitStrategy.SLEEP_100MS;

public class StreamDataCodecTest {

  public static final String QUALIFIER = "qual";
  public static final String PING = "ping";
  public static final String DOUBLE_PING = "pingping";
  public static final int TIMEOUT = 3;

  private StreamMessageDataCodec codec = new StreamMessageDataCodecImpl();

  private ListeningServerStream listeningServerStream;
  private ClientStream client;


  @Before
  public void setUp() {
    listeningServerStream = ListeningServerStream.newListeningServerStream();
    client = ClientStream.newClientStream();
  }

  @After
  public void tearDown() {
    client.close();
    listeningServerStream.close();
  }

  @Test
  public void testDataSerializationInStreams() throws IOException {
    // Given:
    listeningServerStream.listenReadSuccess().map(Event::getMessageOrThrow).subscribe(req -> {
      try {
        StreamMessage deserialized = codec.decodeData(req, String.class);
        String responsePayload = (String) deserialized.data() + deserialized.data();
        StreamMessage toConsumer = codec.encodeData(StreamMessage.from(req).data(responsePayload).build());
        listeningServerStream.send(toConsumer);
      } catch (Throwable e) {
        System.out.println("Err occurred " + e.getMessage());
        Assert.fail();
      }
    });
    Address address = listeningServerStream.bindAwait();
    TestSubscriber<String> test = client.listenReadSuccess()
        .map(e -> (String) codec.decodeData(e.getMessageOrThrow(), String.class).data()).test();


    // When:
    StreamMessage req = codec.encodeData(StreamMessage.builder().qualifier(QUALIFIER).data(PING).build());
    client.send(address, req);

    // Then:
    String response = test.awaitCount(1, SLEEP_100MS)
        .assertValueCount(1)
        .values().get(0);
    Assert.assertEquals(DOUBLE_PING, response);
  }
}
