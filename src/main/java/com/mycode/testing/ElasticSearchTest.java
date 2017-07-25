package com.mycode.testing;

import com.google.common.collect.Lists;
import com.mycode.elasticsearch.ElasticSearchHandler;
import org.apache.http.util.Args;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ElasticSearchTest {

    /*
    *** Context ***
    Supposed in ElasticSearch, index "someIndex" contains millions of documents having 10 fields:
    field1 | field 2 | ... | field 10
     ABCD    UXYZP            HQGTW

    We are intersted in 5 fields (field 1 to field 5).

    We have two input texts: ABC and XYZ.
    Return a hit if ALL those conditions are satisfied:
    1) ABC can match at least one field (from 1 to 5).
    2) XYZ can match at least one field (from 1 to 5).
    *** Func call ***
    testPartialMatchForAnyFields("ABC", "XYZ")
    Return a list of documents satisfying those conditions.
     */
    public static List<Map<String, Object>> testPartialMatchForAnyFields(String... input){

        Args.notNull(input, "input");
        Args.check(input.length > 0, "Must have at least one input.");

        /*
        Equivalent search directly to ES by some RESTful client.
        Link: http://localhost:9200/someIndex/_search
        POST Data:
        {
          "query": {
            "query_string": {
              "fields": ["field1", "field2", "field3", "field4", "field5"],
              "query": "*ABC* AND *XYZ*"
            }
          }
        }
         */

        // Construct the query string.
        String queryString = Arrays.asList(input).stream().map(x -> String.format("*%s*", x)).collect(Collectors.joining(" AND "));

        // Use wildcard to query ElasticSearch.
        QueryBuilder qb = QueryBuilders.queryStringQuery(String.format("%s", queryString))
                .field("field1")
                .field("field2")
                .field("field3")
                .field("field4")
                .field("field5");

        int max_hits = 10000;   // Max capacity from ES for one call.

        SearchResponse response = ElasticSearchHandler.getInstance()
                .getTransportClient()
                .prepareSearch()
                .setIndices("someIndex")
                .setQuery(qb)
                .setFrom(0)
                .setSize(max_hits)
                .execute()
                .actionGet();

        List<Map<String, Object>> res = Lists.newArrayList();

        for(int i = 0; i < response.getHits().internalHits().length; i++){

            Map<String, Object> tempMap = response.getHits().getAt(i).getSource();
            res.add(tempMap);
        }

        return res;
    }
}
