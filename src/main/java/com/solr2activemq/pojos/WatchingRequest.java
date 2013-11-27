package com.solr2activemq.pojos;

/**
 * User: dbraga - Date: 11/26/13
 */
public class WatchingRequest extends Message {
  public static final String INFO = "info";
  public static final String ERROR = "error";
  public static final String WARNING = "warning";

  private String body;
  private int requestInProgress = 0;
  private String errorRequest;

  public WatchingRequest(String body, int requestInProgress, String errorRequest, String msgType){
    this.body = body;
    this.requestInProgress = requestInProgress;
    this.errorRequest = errorRequest;
    setMessageType(msgType);
    setSource(this.getClass().getSimpleName());

  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public int getRequestInProgress() {
    return requestInProgress;
  }

  public void setRequestInProgress(int requestInProgress) {
    this.requestInProgress = requestInProgress;
  }

  public String getErrorRequest() {
    return errorRequest;
  }

  public void setErrorRequest(String errorRequest) {
    this.errorRequest = errorRequest;
  }
}
