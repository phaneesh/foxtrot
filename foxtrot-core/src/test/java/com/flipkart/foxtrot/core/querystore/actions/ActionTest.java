package com.flipkart.foxtrot.core.querystore.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.CacheUtils;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.manager.impl.ElasticsearchIndexStoreManager;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.*;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by rishabh.goyal on 10/12/15.
 */
public class ActionTest {

    public QueryExecutor queryExecutor;
    public ObjectMapper mapper = new ObjectMapper();
    public MockElasticsearchServer elasticsearchServer;
    public HazelcastInstance hazelcastInstance;
    public ElasticsearchQueryStore queryStore;
    public ElasticsearchIndexStoreManager indexStoreManager;

    @Before
    public void setUp() throws Exception {

        DataStore dataStore = TestUtils.getDataStore();
        //Initializing Cache Factory
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance();
        HazelcastConnection hazelcastConnection = Mockito.mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        CacheUtils.setCacheFactory(new DistributedCacheFactory(hazelcastConnection, mapper));

        // Mock elasticsearch connection
        elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
        ElasticsearchConnection elasticsearchConnection = Mockito.mock(ElasticsearchConnection.class);
        when(elasticsearchConnection.getClient()).thenReturn(elasticsearchServer.getClient());

        // Ensure that table exists before saving/reading data from it
        TableMetadataManager tableMetadataManager = Mockito.mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(TestUtils.TEST_TABLE_NAME)).thenReturn(true);
        when(tableMetadataManager.get(anyString())).thenReturn(TestUtils.TEST_TABLE);

        this.indexStoreManager = new ElasticsearchIndexStoreManager(
                elasticsearchConnection, new ElasticsearchConfig(), tableMetadataManager
        );
        indexStoreManager.initializeFoxtrot();
        queryStore = new ElasticsearchQueryStore(tableMetadataManager, indexStoreManager, elasticsearchConnection, dataStore, mapper);
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, queryStore, indexStoreManager, elasticsearchConnection);
        TestUtils.registerActions(analyticsLoader, mapper);
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        queryExecutor = new QueryExecutor(analyticsLoader, executorService);
    }

}
