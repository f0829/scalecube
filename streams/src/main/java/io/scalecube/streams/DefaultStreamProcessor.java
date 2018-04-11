package io.scalecube.streams;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.disposables.CompositeDisposable;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DefaultStreamProcessor implements StreamProcessor {

  public static final StreamMessage onErrorMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_GENERAL_FAILURE).build();

  public static final StreamMessage onCompletedMessage =
      StreamMessage.builder().qualifier(Qualifier.Q_ON_COMPLETED).build();

  private final ChannelContext channelContext;
  private final EventStream eventStream = new DefaultEventStream();

  private final AtomicBoolean isTerminated = new AtomicBoolean(); // state

  /**
   * Constructor for this stream processor.
   * 
   * @param channelContext channel context
   * 
   */
  public DefaultStreamProcessor(ChannelContext channelContext) {
    this.channelContext = channelContext;
    this.eventStream.subscribe(channelContext);
  }

  @Override
  public void onError(Throwable throwable) {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onErrorMessage);
    }
  }

  @Override
  public void onComplete() {
    if (isTerminated.compareAndSet(false, true)) {
      channelContext.postWrite(onCompletedMessage);
    }
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    subscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(StreamMessage message) {
    if (!isTerminated.get()) {
      channelContext.postWrite(message);
    }
  }

  @Override
  public Flowable<StreamMessage> listen() {
    return Flowable.create(emitter -> {

      CompositeDisposable subscriptions = new CompositeDisposable();
      emitter.setCancellable(subscriptions::clear);

      // message logic: remote read => onMessage
      subscriptions.add(
          eventStream.listenReadSuccess()
              .map(Event::getMessageOrThrow)
              .subscribe(message -> onMessage(message, emitter)));

      // error logic: failed remote write => observer error
      subscriptions.add(
          eventStream.listenWriteError()
              .map(Event::getErrorOrThrow)
              .subscribe(emitter::onError));

      // connection logic: connection lost => observer error
      subscriptions.add(
          eventStream.listenChannelContextClosed()
              .filter(event -> event.getAddress().equals(channelContext.getAddress()))
              .map(event -> new IOException("ChannelContext closed on address: " + event.getAddress()))
              .subscribe(emitter::onError));

    }, BackpressureStrategy.BUFFER);
  }

  @Override
  public void close() {
    // this alone will unsubscribe this channel context
    // from local stream => no more requests, no more replies
    channelContext.close();
  }

  private void onMessage(StreamMessage message, FlowableEmitter<StreamMessage> emitter) {
    String qualifier = message.qualifier();
    if (Qualifier.Q_ON_COMPLETED.asString().equalsIgnoreCase(qualifier)) { // remote => onCompleted
      emitter.onComplete();
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
