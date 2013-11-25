package com.solr2activemq;

import com.solr2activemq.messaging.MessagingSystem;
import com.solr2activemq.pojos.ExceptionSolrQuery;
import com.solr2activemq.pojos.SolrQuery;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang.exception.ExceptionUtils;
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;

/**
 * User: dbraga - Date: 11/22/13
 */
public class SolrToActiveMQComponent extends SearchComponent {
  private SolrParams initArgs;

  private static MessagingSystem messagingSystem;
  private boolean needsBootstrap = true;

  private static String ACTIVEMQ_BROKER_URI;
  private static int ACTIVEMQ_BROKER_PORT;
  private static String ACTIVEMQ_DESTINATION_TYPE;
  private static String ACTIVEMQ_DESTINATION_NAME;
  private static String SOLR_HOSTNAME;
  private static int SOLR_PORT;
  private static String SOLR_POOLNAME;
  private static String SOLR_CORENAME;

  private static final String EXCEPTION = "exception";
  private static final String INFO = "info";

  private static ObjectMapper mapper = new ObjectMapper();

  private static final int BUFFER_SIZE = 2;
  private static final int BUFFER_DEQUEUING_POLLING = 5000;
  private static final int CHECK_ACTIVEMQ__POLLING = 5000;


  private static Timer dequeuingTimer = new Timer("dequeuingTimer", true);
  private static Timer checkActiveMQTimer = new Timer("checkActiveMQTimer", true);
  private ReentrantLock lockObj = new ReentrantLock( );
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
      needsBootstrap = false;
    }
    catch (JMSException e){
      System.out.println("SolrToActiveMQComponent: Bootstrapping messaging system failed.\n" + e);
      needsBootstrap = true;
    }
  }

  /**
   * Dequeue all messages in the buffer if not empty and enqueue them to the activeMQ destination
   */
  class DequeueFromBuffer extends TimerTask {
    @Override
    public void run() {
      // Wake up and dequeue a message from the buffer
      if (!circularFifoBuffer.isEmpty() && !lockObj.isLocked() && !needsBootstrap){
        try{
          lockObj.lock();
          while(!circularFifoBuffer.isEmpty()) {
            messagingSystem.sendMessage((TextMessage) circularFifoBuffer.remove());
          }
        } catch (JMSException e){
         // session or connection lost
          needsBootstrap = true;
        }
        finally {
           lockObj.unlock();
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
      if (needsBootstrap){
        bootstrapMessagingSystem();
      }
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

    System.out.println("SolrToActiveMQComponent: loaded configuration:" +
            "\n\tACTIVEMQ_BROKER_URI: " + ACTIVEMQ_BROKER_URI +
            "\n\tACTIVEMQ_BROKER_PORT: " + ACTIVEMQ_BROKER_PORT +
            "\n\tACTIVEMQ_DESTINATION_TYPE: " + ACTIVEMQ_DESTINATION_TYPE +
            "\n\tACTIVEMQ_DESTINATION_NAME: " + ACTIVEMQ_DESTINATION_NAME +
            "\n\tSOLR_HOSTNAME: " + SOLR_HOSTNAME +
            "\n\tSOLR_PORT: " + SOLR_PORT +
            "\n\tSOLR_POOLNAME: " + SOLR_POOLNAME +
            "\n\tSOLR_CORENAME: " + SOLR_CORENAME
    );

    circularFifoBuffer = new CircularFifoBuffer(BUFFER_SIZE);
    bootstrapMessagingSystem();
    dequeuingTimer.schedule(new DequeueFromBuffer(), 0, BUFFER_DEQUEUING_POLLING);
    checkActiveMQTimer.schedule(new CheckIfActiveMQNeedsBootstrap(), 0, CHECK_ACTIVEMQ__POLLING);
  }


  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrQueryResponse rsp = rb.rsp;
    SolrQueryRequest req = rb.req;

    TextMessage message;

    try {
      // Fetch information about the solr query
      SolrQuery solrQuery = new SolrQuery(
              (rsp.getToLog().get("params") == null ) ? "" : (String)rsp.getToLog().get("params"),
              (rsp.getToLog().get("hits") == null )? 0 : (Integer)rsp.getToLog().get("hits"),
              rsp.getEndTime()-req.getStartTime(),
              (rsp.getToLog().get("path") == null ) ? "" : (String)rsp.getToLog().get("path"),
              (rsp.getToLog().get("webapp") == null ) ? "" :(String)rsp.getToLog().get("webapp")
      );
      if (rb.rsp.getException() == null){
        message = addMessageProperties(messagingSystem.getSession().createTextMessage(mapper.writeValueAsString(solrQuery)), INFO);
      } else {
        // The response generated an exception
        ExceptionSolrQuery exceptionSolrQuery = new ExceptionSolrQuery(solrQuery, ExceptionUtils.getStackTrace(rb.rsp.getException()));
        message = addMessageProperties(messagingSystem.getSession().createTextMessage(mapper.writeValueAsString(exceptionSolrQuery)), EXCEPTION);
      }
      // Add the message to the buffer
      circularFifoBuffer.add(message);
    } catch (JMSException e){
      e.printStackTrace();
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {}

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
