package cz.mxmx.servicebustest;

import java.util.concurrent.CompletableFuture;

import com.microsoft.azure.servicebus.ExceptionPhase;
import com.microsoft.azure.servicebus.IMessage;
import com.microsoft.azure.servicebus.IMessageHandler;
import com.microsoft.azure.servicebus.ISubscriptionClient;

public class MessageListener implements IMessageHandler {
  
  private final ISubscriptionClient subscriptionClient;
  
  public MessageListener(ISubscriptionClient subscriptionClient) {
    this.subscriptionClient = subscriptionClient;
  }
  
  @Override
  public CompletableFuture<Void> onMessageAsync(IMessage message) {
    System.out.format("Message received: %s\n", message.toString());
    return subscriptionClient.completeAsync(message.getLockToken());
  }
  
  @Override
  public void notifyException(Throwable exception, ExceptionPhase phase) {
    System.out.format("Exception in phase %s: %s\n", phase, exception);
  }
}
