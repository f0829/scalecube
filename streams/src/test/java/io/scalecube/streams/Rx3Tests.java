package io.scalecube.streams;

import static org.junit.Assert.assertEquals;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableSubscriber;

import org.junit.Test;
import org.reactivestreams.Subscription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Rx3Tests {
  String result = "";

  int N = 5;
  int N_SUBSCRIBERS = 1;

  @Test
  public void testSimple() throws InterruptedException {
    result = "";
    BackpressureStrategy buffer = BackpressureStrategy.ERROR;
    Flowable<Integer> flowable = Flowable.create(flowableEmitter -> {
      IntStream.rangeClosed(1, N).forEach(flowableEmitter::onNext);
      flowableEmitter.onComplete();
    }, buffer);
    AtomicInteger rcvd = new AtomicInteger();
    AtomicInteger subscriberId = new AtomicInteger();

    CountDownLatch latch = new CountDownLatch(N);
    for (int i = 0; i < N_SUBSCRIBERS; i++)
      flowable.subscribeWith(new FlowableSubscriber<Integer>() {
        int id = subscriberId.incrementAndGet();

        @Override
        public void onSubscribe(Subscription s) {
          System.out.println("Subscriber " + id + ": Initialization passed");
          s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(Integer integer) {
          int k = rcvd.incrementAndGet();
          System.out.println("Subscriber " + id + ":" + k);
        }

        @Override
        public void onError(Throwable throwable) {
          latch.countDown();
        }

        @Override
        public void onComplete() {
          System.out.println("Subscriber " + id + ":End");
          latch.countDown();
        }
      });
    latch.await();
    assertEquals(N * N_SUBSCRIBERS, rcvd.get());
  }
}
