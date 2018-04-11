package io.scalecube.gateway.socketio;

import io.scalecube.socketio.Session;
import io.scalecube.socketio.SocketIOListener;
import io.scalecube.streams.ChannelContext;
import io.scalecube.streams.Event;
import io.scalecube.streams.EventStream;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.codec.StreamMessageCodec;
import io.scalecube.streams.netty.ChannelSupport;
import io.scalecube.transport.Address;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * SocketIO listener integrated with {@link EventStream}.
 */
public final class GatewaySocketIoListener implements SocketIOListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(GatewaySocketIoListener.class);

  /**
   * A mapping between socketio {@link Session} identifier and {@link ChannelContext} object. Map is updated when
   * corresponding {@link Session} disconnects.
   */
  private final ConcurrentMap<String, ChannelContext> channelContexts = new ConcurrentHashMap<>();

  private final EventStream eventStream;

  public GatewaySocketIoListener(EventStream eventStream) {
    this.eventStream = eventStream;
  }

  @Override
  public void onConnect(Session session) {
    // create channel context
    InetSocketAddress remoteAddress = (InetSocketAddress) session.getRemoteAddress();
    String host = remoteAddress.getAddress().getHostAddress();
    int port = remoteAddress.getPort();
    ChannelContext channelContext = ChannelContext.create(Address.create(host, port));

    // save mapping
    channelContexts.put(session.getSessionId(), channelContext);

    // register cleanup process upfront
    channelContext.listenClose(input -> {
      if (session.getState() == Session.State.CONNECTED) {
        session.disconnect();
      }
    });

    // bind channelContext
    eventStream.subscribe(channelContext);

    channelContext.listenWrite().map(Event::getMessageOrThrow).subscribe(
        message -> {
          ByteBuf buf = StreamMessageCodec.encode(message);
          ChannelSupport.releaseRefCount(message.data()); // release ByteBuf
          try {
            session.send(buf);
            channelContext.postWriteSuccess(message);
          } catch (Exception throwable) {
            channelContext.postWriteError(message, throwable);
          }
        },
        throwable -> {
          LOGGER.error("Fatal exception occured on channel context: {}", channelContext.getId(), throwable);
          session.disconnect();
        });
  }

  @Override
  public void onMessage(Session session, ByteBuf buf) {
    ChannelContext channelContext = channelContexts.get(session.getSessionId());
    if (channelContext == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      ChannelSupport.releaseRefCount(buf);
      session.disconnect();
      return;
    }

    StreamMessage message = null;
    try {
      message = StreamMessageCodec.decode(buf);
    } catch (Exception throwable) {
      channelContext.postReadError(throwable);
    } finally {
      ChannelSupport.releaseRefCount(buf);
    }

    if (message != null) {
      channelContext.postReadSuccess(message);
    }
  }

  @Override
  public void onDisconnect(Session session) {
    ChannelContext channelContext = channelContexts.remove(session.getSessionId());
    if (channelContext == null) {
      LOGGER.error("Can't find channel context id by session id: {}", session.getSessionId());
      return;
    }
    channelContext.close();
  }
}
