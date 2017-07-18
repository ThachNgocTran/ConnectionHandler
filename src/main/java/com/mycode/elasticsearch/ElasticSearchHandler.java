package com.mycode.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetSocketAddress;

public class ElasticSearchHandler {

    private TransportClient esClient;

    private ElasticSearchHandler(){

        //https://discuss.elastic.co/t/transportclient-functionality/11502/2
        // TransportClient is a connection pool.

        /*
        It takes several seconds to initialize.
         */
        esClient = new PreBuiltTransportClient(Settings.EMPTY);
        esClient.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddresses.forString(
                "ELASTICSEARCH_HOST"),
                Integer.valueOf("ELASTICSEARCH_PORT"))));

        // http://elasticsearch.narkive.com/igQEuQDS/elastic-search-transport-client-singleton
        /*
        You can share a client instance between many concurrent threads. There is
        no need to close transport client connections after actionGet().
         */
    }

    public TransportClient getTransportClient(){
        return esClient;
    }

    private static ElasticSearchHandler instance;

    public static synchronized ElasticSearchHandler getInstance(){

        if (instance == null){
            instance = new ElasticSearchHandler();
        }

        return instance;
    }

    /*
    Utility function to early initialize the TransportClient.
    Optional to be called.
     */
    public static void init(){

        if (instance == null){
            instance = new ElasticSearchHandler();
        }
    }
}
