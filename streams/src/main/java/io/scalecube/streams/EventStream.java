package io.scalecube.streams;

import io.reactivex.Flowable;
import io.scalecube.transport.Address;

import java.util.function.Consumer;

public interface EventStream {

  void subscribe(ChannelContext channelContext);

  io.reactivex.Flowable<Event> listen();

  void onNext(Event event);

  void onNext(Address address, Event event);

  void close();

  void listenClose(Consumer<Void> onClose);

  default Flowable<Event> listenReadSuccess() {
    return listen().filter(Event::isReadSuccess);
  }

  default Flowable<Event> listenReadError() {
    return listen().filter(Event::isReadError);
  }

  default Flowable<Event> listenWrite() {
    return listen().filter(Event::isWrite);
  }

  default Flowable<Event> listenWriteSuccess() {
    return listen().filter(Event::isWriteSuccess);
  }

  default Flowable<Event> listenWriteError() {
    return listen().filter(Event::isWriteError);
  }

  default Flowable<Event> listenChannelContextClosed() {
    return listen().filter(Event::isChannelContextClosed);
  }

  default Flowable<Event> listenChannelContextSubscribed() {
    return listen().filter(Event::isChannelContextSubscribed);
  }

  default Flowable<Event> listenChannelContextUnsubscribed() {
    return listen().filter(Event::isChannelContextUnsubscribed);
  }
}
