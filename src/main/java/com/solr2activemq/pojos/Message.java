package com.solr2activemq.pojos;

/**
 * User: dbraga - Date: 11/26/13
 */
public class Message {
  /**
   * type of message: e.g: information, exception, warning, error
   */
  private String messageType;

  /**
   * who generated the message
   */
  private String source;

  /**
   *
   * @return source of the message
   */
  public String getSource() {
    return source;
  }

  /**
   *
   * @param source who generated the message
   */
  public void setSource(String source) {
    this.source = source;
  }

  /**
   *
   * @return the type of the message
   */
  public String getMessageType() {
    return messageType;
  }

  /**
   *
   * @param messageType type of the message: e.g: information, exception, warning, error
   */
  public void setMessageType(String messageType) {
    this.messageType = messageType;
  }
}
