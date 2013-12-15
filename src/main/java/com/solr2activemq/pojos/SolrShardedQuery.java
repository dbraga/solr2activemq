package com.solr2activemq.pojos;

import org.apache.solr.common.util.NamedList;

/**
 * User: dbraga - Date: 12/11/13
 */
public class SolrShardedQuery extends SolrQuery{
  private NamedList shardsInfo;

  public SolrShardedQuery(String params, int hits, long qtime, String path, String webapp, NamedList shardsInfo) {
    super(params, hits, qtime, path, webapp);
    this.shardsInfo = shardsInfo;
  }

  public NamedList getShardsInfo() {
    return shardsInfo;
  }

  public void setShardsInfo(NamedList shardsInfo) {
    this.shardsInfo = shardsInfo;
  }
}
