package io.scalecube.streams;

import static io.reactivex.schedulers.Schedulers.computation;

import io.reactivex.processors.PublishProcessor;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class Rx3Tests {

  int N = 10_000;

//  @Test
//  public void testSimple() throws InterruptedException {
//    AtomicInteger subscriber = new AtomicInteger();
//    PublishProcessor<Integer> source = PublishProcessor.create();
//    source
//        .observeOn(computation())
//        .buffer(1000)
//        .subscribe(
//            i -> heavyCompute(subscriber.getAndIncrement(), i),
//            t -> t.printStackTrace());
//    IntStream.rangeClosed(1, N).forEach(source::onNext);
//    Thread.currentThread().join();
//  }
//
//  static int heavyCompute(int subId, int i) {
//    System.out.println("Subscriber:" + subId + " processing:" + i);
//    return compute(1_000_000);
//  }
//
//  static int lightCompute() {
//    return compute(10);
//  }
//
//  private static int compute(int i2) {
//    int n = i2;
//    for (int i = 0; i < n; i++) {
//      Math.hypot(1.0, 1.0);
//    }
//    return n;
//  }

}
