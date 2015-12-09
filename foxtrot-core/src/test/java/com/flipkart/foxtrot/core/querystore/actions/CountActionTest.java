package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CountActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        List<Document> documents = TestUtils.getCountDocuments(mapper);
        queryStore.save(TestUtils.TEST_TABLE_NAME, documents);
        for (Document document : documents) {
            elasticsearchServer.getClient().admin().indices()
                    .prepareRefresh(indexStoreManager.getIndexNameForTimestamp(TestUtils.TEST_TABLE_NAME, document.getTimestamp()))
                    .setForce(true).execute().actionGet();
        }
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    @Test
    public void testCount() throws QueryStoreException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(11, countResponse.getCount());
    }

    @Test
    public void testCountWithFilter() throws QueryStoreException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(false);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    @Test
    public void testCountDistinct() throws QueryStoreException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilter() throws QueryStoreException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("device", "nexus"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));

        assertNotNull(countResponse);
        assertEquals(2, countResponse.getCount());
    }

    @Test
    public void testCountDistinctWithFilterOnSameField() throws QueryStoreException {
        CountRequest countRequest = new CountRequest();
        countRequest.setTable(TestUtils.TEST_TABLE_NAME);
        countRequest.setField("os");
        ArrayList<Filter> filters = new ArrayList<Filter>();
        filters.add(new EqualsFilter("os", "android"));
        countRequest.setFilters(filters);
        countRequest.setDistinct(true);
        CountResponse countResponse = CountResponse.class.cast(queryExecutor.execute(countRequest));
        assertNotNull(countResponse);
        assertEquals(1, countResponse.getCount());
    }


}