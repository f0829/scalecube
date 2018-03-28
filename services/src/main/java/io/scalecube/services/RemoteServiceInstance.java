package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.streams.Qualifier;
import io.scalecube.streams.StreamMessage;
import io.scalecube.streams.StreamProcessor;
import io.scalecube.streams.StreamProcessors.ClientStreamProcessors;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RemoteServiceInstance implements ServiceInstance {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteServiceInstance.class);

  private final Address address;
  private final String memberId;
  private final String serviceName;
  private final Map<String, String> tags;
  private final ClientStreamProcessors client;
  private final Set<String> methods;

  /**
   * Remote service instance constructor to initiate instance.
   * 
   * @param client to be used for communication.
   * @param serviceReference service reference of this instance.
   * @param tags describing this service instance metadata.
   */
  public RemoteServiceInstance(ClientStreamProcessors client, ServiceReference serviceReference,
      Map<String, String> tags) {

    this.serviceName = serviceReference.serviceName();
    this.methods = Collections.unmodifiableSet(serviceReference.methods());
    this.address = serviceReference.address();
    this.memberId = serviceReference.memberId();
    this.tags = tags;
    this.client = client;
  }

  @Override
  public Observable<Message> listen(final Message request, Duration duration) {
    return this.listen(fromMessage(request), duration)
        .map(func -> toMessage(func));
  }

  @Override
  public Observable<StreamMessage> listen(final StreamMessage request, Duration duration) {

    StreamProcessor sp = client.create(address);
    Observable<StreamMessage> observer = sp.listen()
        .timeout(duration.toMillis(), TimeUnit.MILLISECONDS);

    sp.onNext(request);
    sp.onCompleted();
    return observer;

  }

  @Override
  public CompletableFuture<Message> invoke(Message request, Duration duration) {
    Messages.validate().serviceRequest(request);
    CompletableFuture<Message> result = new CompletableFuture<Message>();

    this.invoke(fromMessage(request), duration)
        .whenComplete((value, error) -> {
          if (error == null) {
            result.complete(toMessage(value));
          } else {
            result.completeExceptionally(error);
          }
        });

    return result;
  }

  @Override
  public CompletableFuture<StreamMessage> invoke(StreamMessage request, Duration duration) {

    CompletableFuture<StreamMessage> result = new CompletableFuture<StreamMessage>();

    StreamProcessor sp = client.create(address);
    Observable<StreamMessage> observer = sp.listen()
        .timeout(duration.toMillis(), TimeUnit.MILLISECONDS);

    sp.onNext(request);
    sp.onCompleted();

    observer.subscribe(onNext -> {
      result.complete(onNext);
    }, onError -> {
      LOGGER.error("Failed to send request {} to target address {}", request, address);
      result.completeExceptionally(onError);
    }, () -> {
      result.complete(null);
    });

    return result;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  public Address address() {
    return address;
  }

  @Override
  public Boolean isLocal() {
    return false;
  }

  @Override
  public Map<String, String> tags() {
    return tags;
  }

  @Override
  public String serviceName() {
    return serviceName;
  }

  @Override
  public boolean methodExists(String methodName) {
    return methods.contains(methodName);
  }

  @Override
  public void checkMethodExists(String methodName) {
    checkArgument(this.methodExists(methodName), "instance has no such requested method");
  }

  @Override
  public Collection<String> methods() {
    return methods;
  }

  private static Message toMessage(StreamMessage request) {
    Qualifier qualifier = Messages.qualifierOf(request);

    return Message.builder()
        .header(ServiceHeaders.SERVICE_REQUEST, qualifier.getAction())
        .header(ServiceHeaders.METHOD, qualifier.getNamespace())
        .data(request.data()).build();

  }

  private static StreamMessage fromMessage(Message request) {
    Qualifier qualifier = Messages.qualifierOf(request);
    return StreamMessage.builder()
        .qualifier(qualifier)
        .data(request.data())
        .build();
  }


  @Override
  public String toString() {
    return "RemoteServiceInstance [serviceName=" + serviceName + ", address=" + address + ", memberId=" + memberId
        + ", methods=" + methods + ", tags=" + tags + "]";
  }


}
