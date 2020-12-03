package cz.mxmx.servicebustest;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import com.microsoft.azure.servicebus.ISubscriptionClient;
import com.microsoft.azure.servicebus.MessageHandlerOptions;
import com.microsoft.azure.servicebus.ReceiveMode;
import com.microsoft.azure.servicebus.SubscriptionClient;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

public class App {
  
  // Connection config
  private static final String CONNECTION_STRING = "TODO - connection string to the SB here";
  private static final String TOPIC = "topic";
  private static final String SUBSCRIPTION = "subscription";
  
  private static final ReceiveMode DEFAULT_RECEIVE_MODE = ReceiveMode.PEEKLOCK;
  private static final Duration MAX_RENEW_TIME_MINUTES = Duration.ofMinutes(5);
  private static final String SUBSCRIPTION_PATH = "%s/subscriptions/%s";
  private static final boolean AUTO_COMPLETE = false;
  private static final int NUMBER_OF_LISTENERS = 32;
  private static final int MAX_CONCURRENT_CALLS = 1;
  private static final int THREAD_POOL_SIZE = 12;
  
  private final ExecutorService executorService;
  
  public static void main(String[] args) {
    ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    App app = new App(executorService);
    app.connect();
  }
  
  public App(ExecutorService executorService) {
    this.executorService = executorService;
  }
  
  public void connect() {
    IntStream
        .range(0, NUMBER_OF_LISTENERS)
        .boxed()
        .parallel() // make things faster for larger NUMBER_OF_LISTENERS; feel free to remove the line
        .forEach(i -> {
          try {
            ISubscriptionClient client = getClient(TOPIC, SUBSCRIPTION);
            client.registerMessageHandler(new MessageListener(client), getHandlerOptions(), executorService);
            System.out.printf("Created listener #%d\n", i);
          } catch (Exception e) {
            e.printStackTrace(); // whatever
          }
        });
    
    System.out.println("Done");
  }
  
  public ISubscriptionClient getClient(String topic, String sub) throws ServiceBusException, InterruptedException {
    ConnectionStringBuilder connectionStringBuilder = new ConnectionStringBuilder(
        CONNECTION_STRING,
        getSubscriptionPath(topic, sub)
    );
    
    return new SubscriptionClient(connectionStringBuilder, DEFAULT_RECEIVE_MODE);
  }
  
  private String getSubscriptionPath(String topic, String subscription) {
    return String.format(SUBSCRIPTION_PATH, topic, subscription);
  }
  
  private MessageHandlerOptions getHandlerOptions() {
    return new MessageHandlerOptions(MAX_CONCURRENT_CALLS, AUTO_COMPLETE, MAX_RENEW_TIME_MINUTES);
  }
}
