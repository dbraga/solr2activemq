package com.solr2activemq;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.SearchHandler;
import org.apache.solr.request.SolrQueryRequest;
import java.util.Arrays;
import java.util.List;

/**
 * User: dbraga - Date: 11/23/13
 */
public class SolrToActiveMQHandler extends SearchHandler {

  public void handleRequestBody(SolrQueryRequest req, org.apache.solr.response.SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException {
    try {
      super.handleRequestBody(req,rsp);
    }
    catch(Exception e){
      // Call the solrToActiveMQComponent directly and pass the exception
      SearchComponent solrToActiveMQComponent = new SolrToActiveMQComponent();
      List<SearchComponent> singleComponent = Arrays.asList(solrToActiveMQComponent);
      rsp.setException(e);
      ResponseBuilder rb = new ResponseBuilder(req, rsp, singleComponent);
      solrToActiveMQComponent.process(rb);
      // Finally throw the exception
      throw e;
    }
  }

}
