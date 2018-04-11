package io.scalecube.streams;

import io.reactivex.Flowable;
import org.reactivestreams.Subscriber;
import rx.Observer;

public interface StreamProcessor extends Subscriber<StreamMessage> {

  Flowable<StreamMessage> listen();

  void close();

}
