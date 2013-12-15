package com.solr2activemq.messaging;
import org.apache.activemq.*;

import javax.jms.*;
import javax.jms.Message;

/**
 * User: dbraga - Date: 11/19/13
 */
public class MessagingSystem {


  private ActiveMQConnectionFactory connectionFactory;
  private Connection connection;
  private Session session;
  private Destination destination;
  private MessageConsumer consumer;
  private MessageProducer producer;
  private boolean validConnection;

  public MessagingSystem(String uri, int port){
    this.connectionFactory =  new ActiveMQConnectionFactory("failover:(tcp://"+ uri + ":" + port+")?"+
            "startupMaxReconnectAttempts=1" +
            "&maxReconnectAttempts=1"+
            "&maxReconnectDelay=100");
  }

  public MessagingSystem(ActiveMQConnectionFactory connectionFactory){
    this.connectionFactory = connectionFactory;
  }

  public boolean isValidConnection() {
    return this.validConnection;
  }

  public void validateConnection() {
    this.validConnection = true;
  }

  public void invalidateConnection(){
    this.validConnection = false;
  }

  public void createConnection() throws JMSException {
    // Create a Connection
    connection = connectionFactory.createConnection();
    connection.start();
  }

  public void createSession() throws JMSException {
    // Create a Session
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  //public void createDestination() throws JMSException {
  //  // Create the destination (Queue)
  //  destination = session.createQueue(Configuration.ACTIVEMQ_QUEUE_THOTH_NAME);
  //}

  public void createDestination(String dest) throws JMSException {
    // Create the destination (Queue)
    destination = session.createQueue(dest);
  }

  public void createConsumer() throws JMSException {
    // Create a MessageConsumer from the Session to the Topic or Queue
    consumer = session.createConsumer(destination);
  }

  public void createProducer() throws JMSException {
    // Create a MessageProducer
    producer = session.createProducer(destination);
  }

  public Message getMessage() throws JMSException {
    return consumer.receive(10000);
  }

  public void sendMessage(Message msg) throws JMSException {
    producer.send(destination, msg);
  }

  public String getTextMessage(Message message) throws JMSException {
    TextMessage textMessage = (TextMessage) message;
    return  textMessage.getText();
  }

  public ActiveMQConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  public Connection getConnection() {
    return connection;
  }

  public Session getSession() {
    return session;
  }

  public Destination getDestination() {
    return destination;
  }

  public MessageConsumer getConsumer() {
    return consumer;
  }

  public MessageProducer getProducer() {
    return producer;
  }
}
