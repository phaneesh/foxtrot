package com.flipkart.foxtrot.core.querystore.impl;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.datetime.LastFilter;
import com.flipkart.foxtrot.common.query.numeric.BetweenFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterEqualFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.query.numeric.LessThanFilter;
import com.flipkart.foxtrot.core.MockElasticsearchServer;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.flipkart.foxtrot.core.manager.impl.ElasticsearchIndexStoreManager;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.yammer.dropwizard.util.Duration;
import org.joda.time.DateTimeZone;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;


public class ElasticsearchIndexStoreManagerTest {
    private static final long TEST_CURRENT_TIME = 1428151913000L; //4/4/2015, 6:21:53 PM IST

    @Rule
    public TestWatcher tzRule = new TestWatcher() {

        private DateTimeZone defaultTz = DateTimeZone.getDefault();

        @Override
        protected void starting(Description description) {
            DateTimeZone.setDefault(DateTimeZone.forOffsetHoursMinutes(5, 30));
        }

        @Override
        protected void finished(Description description) {
            DateTimeZone.setDefault(defaultTz);
        }
    };

    private final static class TestRequest extends ActionRequest {

    }

    private ElasticsearchIndexStoreManager indexStoreManager;

    @Before
    public void setUp() throws Exception {
        // Mock elasticsearch connection
        MockElasticsearchServer elasticsearchServer = new MockElasticsearchServer(UUID.randomUUID().toString());
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
    }

    @Test
    public void testGetIndicesLastSameSDay() throws Exception {
        TestRequest request = new TestRequest();
        LastFilter filter = new LastFilter();
        filter.setDuration(Duration.minutes(10));
        filter.setCurrentTime(TEST_CURRENT_TIME);
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{"foxtrot-test-table-04-4-2015"}, indexes);
    }

    @Test
    public void testGetIndicesLastLastDays() throws Exception {
        TestRequest request = new TestRequest();
        LastFilter filter = new LastFilter();
        filter.setDuration(Duration.days(2));
        filter.setCurrentTime(TEST_CURRENT_TIME);
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{
                "foxtrot-test-table-02-4-2015",
                "foxtrot-test-table-03-4-2015",
                "foxtrot-test-table-04-4-2015"}, indexes);
    }

    @Test
    public void testGetIndicesBetween() throws Exception {
        TestRequest request = new TestRequest();
        BetweenFilter filter = new BetweenFilter();
        filter.setTemporal(true);
        filter.setFrom(1427997600000L); //4/2/2015, 11:30:00 PM IST
        filter.setTo(1428001200000L);   //4/3/2015, 12:30:00 AM IST
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{
                "foxtrot-test-table-02-4-2015",
                "foxtrot-test-table-03-4-2015"}, indexes);
    }

    @Test
    public void testGetIndicesGreaterThan() throws Exception {
        TestRequest request = new TestRequest();
        GreaterThanFilter filter = new GreaterThanFilter();
        filter.setTemporal(true);
        filter.setValue(1427997600000L); //4/2/2015, 11:30:00 PM IST
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{
                "foxtrot-test-table-02-4-2015",
                "foxtrot-test-table-03-4-2015",
                "foxtrot-test-table-04-4-2015"}, indexes);
    }

    @Test
    public void testGetIndicesGreaterEquals() throws Exception {
        TestRequest request = new TestRequest();
        GreaterEqualFilter filter = new GreaterEqualFilter();
        filter.setTemporal(true);
        filter.setValue(1427997600000L); //4/2/2015, 11:30:00 PM IST
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{
                "foxtrot-test-table-02-4-2015",
                "foxtrot-test-table-03-4-2015",
                "foxtrot-test-table-04-4-2015"}, indexes);
    }

    @Test
    public void testGetIndicesLessThan() throws Exception {
        TestRequest request = new TestRequest();
        LessThanFilter filter = new LessThanFilter();
        filter.setTemporal(true);
        filter.setValue(1427997600000L); //4/2/2015, 11:30:00 PM IST
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{"foxtrot-test-table-*"}, indexes);
    }

    @Test
    public void testGetIndicesLessThanEquals() throws Exception {
        TestRequest request = new TestRequest();
        LessThanFilter filter = new LessThanFilter();
        filter.setTemporal(true);
        filter.setValue(1427997600000L); //4/2/2015, 11:30:00 PM IST
        request.setFilters(Collections.<Filter>singletonList(filter));
        String indexes[] = indexStoreManager.getIndices("test", request,
                new PeriodSelector(request.getFilters()).analyze(TEST_CURRENT_TIME));
        System.out.println(Arrays.toString(indexes));
        Assert.assertArrayEquals(new String[]{"foxtrot-test-table-*"}, indexes);
    }

}