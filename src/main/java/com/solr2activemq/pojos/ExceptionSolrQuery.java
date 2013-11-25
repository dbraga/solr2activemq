package com.solr2activemq.pojos;


/**
 * User: dbraga - Date: 10/30/13
 */
public class ExceptionSolrQuery extends SolrQuery{
  private String stackTrace;

  public ExceptionSolrQuery(String params, int hits, long qtime, String path, String webapp, String stackTrace){
    super(params,hits,qtime,path,webapp);
    this.stackTrace = stackTrace;
  }

  public ExceptionSolrQuery(SolrQuery solrQuery, String stackTrace){
    super(solrQuery.getParams(),solrQuery.getHits(),solrQuery.getQtime(), solrQuery.getPath(),solrQuery.getWebapp());
    this.stackTrace = stackTrace;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public void setStackTrace(String stackTrace) {
    this.stackTrace = stackTrace;
  }

}
