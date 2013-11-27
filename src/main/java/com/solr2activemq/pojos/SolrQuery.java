package com.solr2activemq.pojos;

import org.joda.time.DateTime;

/**
 * User: dbraga - Date: 11/20/13
 */
public class SolrQuery extends Message {

  private String params;
  private int hits;
  private long qtime;
  private String path;
  private String webapp;
  private DateTime timestamp;
  private static final String INFO = "info";

  public SolrQuery(String params, int hits, long qtime, String path, String webapp){
    this.params = params;
    this.hits = hits;
    this.qtime = qtime;
    this.path = path;
    this.webapp = webapp;
    this.timestamp = DateTime.now();
    setMessageType(INFO);
    setSource(this.getClass().getSimpleName());
  }

  public String getWebapp() {
    return webapp;
  }

  public void setWebapp(String webapp) {
    this.webapp = webapp;
  }


  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public long getQtime() {
    return qtime;
  }

  public void setQtime(int qtime) {
    this.qtime = qtime;
  }

  public int getHits() {
    return hits;
  }

  public void setHits(int hits) {
    this.hits = hits;
  }

  public String getParams() {
    return params;
  }

  public void setParams(String params) {
    this.params = params;
  }
}
