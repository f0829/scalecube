package io.scalecube.streams;

import rx.Emitter;
import rx.Observable;
import rx.Observer;
import rx.subscriptions.CompositeSubscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultStreamProcessor implements StreamProcessor {

  public static final StreamMessage onErrorMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_GENERAL_FAILURE).build();

  public static final StreamMessage onCompletedMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext thisChannelContext;
  private final EventStream otherEventStream;
  private final EventStream thisEventStream;

  private final AtomicBoolean isTerminated = new AtomicBoolean(); // state

  /**
   * Constructor for this stream processor.
   *
   * @param channelContext channel context
   * @param otherEventStream provided event stream
   */
  public DefaultStreamProcessor(ChannelContext channelContext, EventStream otherEventStream) {
    this.thisChannelContext = channelContext;
    this.otherEventStream = otherEventStream;
    this.thisEventStream = new DefaultEventStream();
    // bind channel context to event stream
    this.otherEventStream.subscribe(thisChannelContext);
    this.thisEventStream.subscribe(thisChannelContext);
  }

  @Override
  public void onCompleted() {
    if (isTerminated.compareAndSet(false, true)) {
      thisChannelContext.postWrite(onCompletedMessage);
    }
  }

  @Override
  public void onError(Throwable throwable) {
    if (isTerminated.compareAndSet(false, true)) {
      thisChannelContext.postWrite(onErrorMessage);
    }
  }

  @Override
  public void onNext(StreamMessage message) {
    if (!isTerminated.get()) {
      thisChannelContext.postWrite(message);
    }
  }

  @Override
  public Observable<StreamMessage> listen() {
    return Observable.create(emitter -> {

      CompositeSubscription subscriptions = new CompositeSubscription();
      emitter.setCancellation(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          thisEventStream.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          thisEventStream.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          otherEventStream.listenChannelContextClosed()
              .filter(event -> event.getAddress().equals(thisChannelContext.getAddress()))
              .map(event -> new IOException("ChannelContext closed on address: " + event.getAddress()))
              .subscribe(emitter::onError));

    }, Emitter.BackpressureMode.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    thisChannelContext.close();
  }

  private void onMessage(StreamMessage message, Observer<StreamMessage> emitter) {
    String qualifier = message.qualifier();
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier)) { // remote => onCompleted
      emitter.onCompleted();
      return;
    }
    String qualifierNamespace = Qualifier.getQualifierNamespace(qualifier);
    if (Qualifier.Q_ERROR_NAMESPACE.equalsIgnoreCase(qualifierNamespace)) { // remote => onError
      // Hint: at this point more sophisticated exception mapping logic is needed
      emitter.onError(new RuntimeException(qualifier));
      return;
    }
    emitter.onNext(message); // remote => normal response
  }
}
