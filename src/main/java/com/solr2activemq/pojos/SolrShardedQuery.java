package com.solr2activemq.pojos;

/**
 * User: dbraga - Date: 12/11/13
 */
public class SolrShardedQuery extends SolrQuery{
  private String shardsInfo;

  public SolrShardedQuery(String params, int hits, long qtime, String path, String webapp, String shardsInfo) {
    super(params, hits, qtime, path, webapp);
    this.shardsInfo = shardsInfo;
  }

  public String getShardsInfo() {
    return shardsInfo;
  }

  public void setShardsInfo(String shardsInfo) {
    this.shardsInfo = shardsInfo;
  }
}
