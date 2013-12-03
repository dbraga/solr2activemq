package com.solr2activemq;

import com.solr2activemq.messaging.MessagingSystem;
import com.solr2activemq.pojos.ExceptionSolrQuery;
import com.solr2activemq.pojos.Message;
import com.solr2activemq.pojos.SolrQuery;
import org.apache.commons.collections.BufferUnderflowException;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.codehaus.jackson.map.ObjectMapper;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.solr.util.SolrPluginUtils.docListToSolrDocumentList;

/**
 * User: dbraga - Date: 11/22/13
 */
public class SolrToActiveMQComponent extends SearchComponent {
  private SolrParams initArgs;

  private static MessagingSystem messagingSystem;
  private static String ACTIVEMQ_BROKER_URI;
  private static int ACTIVEMQ_BROKER_PORT;
  private static String ACTIVEMQ_DESTINATION_TYPE;
  private static String ACTIVEMQ_DESTINATION_NAME;
  private static String SOLR_HOSTNAME;
  private static int SOLR_PORT;
  private static String SOLR_POOLNAME;
  private static String SOLR_CORENAME;
  protected static ObjectMapper mapper = new ObjectMapper();

  private static int BUFFER_SIZE;
  private static int CHECK_ACTIVEMQ__POLLING;
  private static int DEQUEUING_FROM_BUFFER_THREAD_POOL_SIZE;

  private static Timer checkActiveMQTimer = new Timer("checkActiveMQTimer", true);


  private static CircularFifoBuffer circularFifoBuffer;


  /**
   * Add message properties to an existing TextMessage
   * @param message an existing TextMessage
   * @param msgType type of message if exception or information
   * @return
   * @throws JMSException
   */
  public static TextMessage addMessageProperties(TextMessage message, String msgType) throws JMSException {
    message.setStringProperty("hostname", SOLR_HOSTNAME);
    message.setIntProperty("port", SOLR_PORT);
    message.setStringProperty("poolName", SOLR_POOLNAME);
    message.setStringProperty("coreName", SOLR_CORENAME);
    message.setStringProperty("msgType", msgType);
    return message;
  }

  /**
   * Creates a new connection and a new session to activeMQ
   */
  public void bootstrapMessagingSystem(){
    try{
      messagingSystem = new MessagingSystem(ACTIVEMQ_BROKER_URI, ACTIVEMQ_BROKER_PORT);
      messagingSystem.createConnection();
      messagingSystem.createSession();
      //TODO: handle topics, not just queues
      messagingSystem.createDestination(ACTIVEMQ_DESTINATION_NAME);
      messagingSystem.createProducer();
      System.out.println("SolrToActiveMQComponent: Bootstrapping messaging system done.");
      messagingSystem.validateConnection();
    }
    catch (JMSException e){
      System.out.println("SolrToActiveMQComponent: Bootstrapping messaging system failed.\n" + e);
      messagingSystem.invalidateConnection();
    }
  }

  /**
   * Dequeue all messages in the buffer if not empty and enqueue them to the activeMQ destination
   */
  class DequeueFromBuffer extends Thread{
    @Override
    public void run() {
      TextMessage message = null;
      while(true){
        synchronized (circularFifoBuffer){
          try {
            while (circularFifoBuffer.isEmpty()) {
              circularFifoBuffer.wait();
            }
            if (messagingSystem.isValidConnection()) { // Dequeing from the buffer only if i can send the message right after
              message = (TextMessage) circularFifoBuffer.remove();
            }
          }
          catch (InterruptedException e) {}
          catch (BufferUnderflowException e) {}
        }
        if (messagingSystem.isValidConnection()){
          try {
            messagingSystem.sendMessage(message);
          } catch (JMSException e) {
            // session or connection lost
            messagingSystem.invalidateConnection();
          }
        }
    }
  }
}

  /**
   * Check if activeMQ connection or session needs bootstrap
   */
  class CheckIfActiveMQNeedsBootstrap extends TimerTask {
    @Override
    public void run() {
      // Wake up and check if activeMQ connection needs to be bootstrapped
      if (!messagingSystem.isValidConnection()){
        bootstrapMessagingSystem();
      }
    }
  }

  /**
   * add a text message to the internal circular fifo buffer
   *
   * @param msg a text message
   */
  public static void addMessageToBuffer(TextMessage msg){
    if (msg != null) {
      // Add the message to the buffer
      synchronized(circularFifoBuffer){
        circularFifoBuffer.add(msg);
        circularFifoBuffer.notify();
      }
    }
  }

  /**
   * Create a text message from a pojo
   *
   * @param pojo representation of the message
   * @return a text message
   */
  public static TextMessage createMessage(Object pojo){
    TextMessage msg = null;
    try {
      if (pojo != null) {
        msg = addMessageProperties(messagingSystem.getSession().createTextMessage(
                mapper.writeValueAsString(pojo)),
                ((Message)pojo).getMessageType()
        );
      }
    } catch (Exception e){
      messagingSystem.invalidateConnection();
    }
    finally {
      return msg;
    }
  }


  @Override
  public void init( NamedList args )
  {
    this.initArgs = SolrParams.toSolrParams(args);
    // Retrieve configuration

    // ActiveMQ configuration
    ACTIVEMQ_BROKER_URI = initArgs.get("activemq-broker-uri", "localhost");
    ACTIVEMQ_BROKER_PORT = initArgs.getInt("activemq-broker-port", 61616);
    ACTIVEMQ_DESTINATION_TYPE = initArgs.get("activemq-broker-destination-type", "queue");
    ACTIVEMQ_DESTINATION_NAME = initArgs.get("activemq-broker-destination-name", "solr_to_activemq_queue");

    // Solr configuration
    SOLR_HOSTNAME = initArgs.get("solr-hostname", "localhost");
    SOLR_PORT = initArgs.getInt("solr-port", 8983);
    SOLR_POOLNAME = initArgs.get("solr-poolname", "default");
    SOLR_CORENAME = initArgs.get("solr-corename", "collection");

    // Solr2ActiveMQ configuration
    BUFFER_SIZE = initArgs.getInt("solr2activemq-buffer-size", 10000);
    DEQUEUING_FROM_BUFFER_THREAD_POOL_SIZE = initArgs.getInt("solr2activemq-dequeuing-from-buffer-pool-size", 4);
    CHECK_ACTIVEMQ__POLLING = initArgs.getInt("solr2activemq-check-activemq-polling", 5000);

    System.out.println("SolrToActiveMQComponent: loaded configuration:" +
            "\n\tACTIVEMQ_BROKER_URI: " + ACTIVEMQ_BROKER_URI +
            "\n\tACTIVEMQ_BROKER_PORT: " + ACTIVEMQ_BROKER_PORT +
            "\n\tACTIVEMQ_DESTINATION_TYPE: " + ACTIVEMQ_DESTINATION_TYPE +
            "\n\tACTIVEMQ_DESTINATION_NAME: " + ACTIVEMQ_DESTINATION_NAME +
            "\n\tSOLR_HOSTNAME: " + SOLR_HOSTNAME +
            "\n\tSOLR_PORT: " + SOLR_PORT +
            "\n\tSOLR_POOLNAME: " + SOLR_POOLNAME +
            "\n\tSOLR_CORENAME: " + SOLR_CORENAME +
            "\n\tBUFFER_SIZE: " + BUFFER_SIZE +
            "\n\tDEQUEUING_FROM_BUFFER_THREAD_POOL_SIZE: " + DEQUEUING_FROM_BUFFER_THREAD_POOL_SIZE +
            "\n\tCHECK_ACTIVEMQ__POLLING: " + CHECK_ACTIVEMQ__POLLING
    );

    circularFifoBuffer = new CircularFifoBuffer(BUFFER_SIZE);
    bootstrapMessagingSystem();
    ExecutorService pool = Executors.newFixedThreadPool(4);
    for (int i=0;i< DEQUEUING_FROM_BUFFER_THREAD_POOL_SIZE;i++){
      pool.submit(new DequeueFromBuffer(),false);
    }

    pool.shutdown();


    checkActiveMQTimer.schedule(new CheckIfActiveMQNeedsBootstrap(), 0, CHECK_ACTIVEMQ__POLLING);
  }


  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}


  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrQueryResponse rsp = rb.rsp;
    SolrQueryRequest req = rb.req;
    SolrDocumentList solrDocumentList = null;
    TextMessage message;


    if (rb.rsp.getException() == null) { // response did not generate an exception
        solrDocumentList = docListToSolrDocumentList(rb.getResults().docList, rb.req.getSearcher(),  new HashSet<String>(), new HashMap(rb.getResults().docList.size()));
    }

    // Fetch information about the solr query
    SolrQuery solrQuery = new SolrQuery(
              (rsp.getToLog().get("params") == null ) ? "" : (String)rsp.getToLog().get("params"),
              (solrDocumentList == null ) ? 0 : (int) solrDocumentList.getNumFound(),
              rsp.getEndTime()-req.getStartTime(),
              (rsp.getToLog().get("path") == null ) ? "" : (String)rsp.getToLog().get("path"),
              (rsp.getToLog().get("webapp") == null ) ? "" :(String)rsp.getToLog().get("webapp")
    );
    if (rb.rsp.getException() == null) { // response did not generate an exception
      message = createMessage(solrQuery);
    } else {
      // The response generated an exception
      ExceptionSolrQuery exceptionSolrQuery = new ExceptionSolrQuery(solrQuery, ExceptionUtils.getStackTrace(rb.rsp.getException()));
      message = createMessage(exceptionSolrQuery);
    }
    addMessageToBuffer(message);
  }

  @Override
  public String getDescription() {
    return null;
  }

  @Override
  public String getSourceId() {
    return null;
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public String getVersion() {
    return null;
  }



}
