package io.scalecube.streams;

import io.scalecube.transport.Address;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.processors.PublishProcessor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class DefaultEventStream implements EventStream {

  private final PublishProcessor<Event> subject = PublishProcessor.create();
  private final PublishProcessor<Event> closeSubject = PublishProcessor.create();

  private final ConcurrentMap<ChannelContext, Disposable> subscriptions = new ConcurrentHashMap<>();

  private final Function<Event, Event> eventMapper;

  public DefaultEventStream() {
    this(Function.identity());
  }

  public DefaultEventStream(Function<Event, Event> eventMapper) {
    this.eventMapper = eventMapper;
  }

  @Override
  public final void subscribe(ChannelContext channelContext) {
    AtomicBoolean valueComputed = new AtomicBoolean();
    subscriptions.computeIfAbsent(channelContext, channelContext1 -> {
      Disposable subscription = subscribe0(channelContext1);
      valueComputed.set(true);
      return subscription;
    });
    if (valueComputed.get()) { // computed in lambda
      channelContext.listenClose(this::onChannelContextClosed);
      onChannelContextSubscribed(channelContext);
    }
  }

  @Override
  public final Flowable<Event> listen() {
    return subject.map(eventMapper::apply);
  }

  @Override
  public void onNext(Event event) {
    subject.onNext(event);
  }

  @Override
  public void onNext(Address address, Event event) {
    Stream<ChannelContext> channelContextStream = subscriptions.keySet().stream();
    channelContextStream.filter(ctx -> ctx.getAddress().equals(address)).forEach(ctx -> ctx.onNext(event));
  }

  @Override
  public final void close() {
    // cleanup subscriptions
    for (ChannelContext channelContext : subscriptions.keySet()) {
      Disposable subscription = subscriptions.remove(channelContext);
      if (subscription != null) {
        subscription.dispose();
      }
    }
    subscriptions.clear();
    // complete subjects
    subject.onComplete();
    closeSubject.onComplete();
  }

  @Override
  public final void listenClose(Consumer<Void> onClose) {
    closeSubject.subscribe(event -> {
    }, throwable -> onClose.accept(null), () -> onClose.accept(null));
  }

  private Disposable subscribe0(ChannelContext channelContext) {
    return channelContext.listen()
        .doOnSubscribe(onSubscribe -> onChannelContextUnsubscribed(channelContext))
        .subscribe(subject::onNext);
  }

  private void onChannelContextClosed(ChannelContext ctx) {
    subject.onNext(Event.channelContextClosed(ctx.getAddress()).identity(ctx.getId()).build());
  }

  private void onChannelContextSubscribed(ChannelContext ctx) {
    subject.onNext(Event.channelContextSubscribed(ctx.getAddress()).identity(ctx.getId()).build());
  }

  private void onChannelContextUnsubscribed(ChannelContext ctx) {
    subject.onNext(Event.channelContextUnsubscribed(ctx.getAddress()).identity(ctx.getId()).build());
  }
}
