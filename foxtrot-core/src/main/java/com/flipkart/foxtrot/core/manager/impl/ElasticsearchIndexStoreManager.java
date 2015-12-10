package com.flipkart.foxtrot.core.manager.impl;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.core.common.PeriodSelector;
import com.flipkart.foxtrot.core.manager.IndexStoreManager;
import com.flipkart.foxtrot.core.manager.StoreManagerException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.google.common.collect.Lists;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by rishabh.goyal on 09/12/15.
 */
public class ElasticsearchIndexStoreManager implements IndexStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchIndexStoreManager.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("dd-M-yyyy");
    private static final String TABLENAME_POSTFIX = "table";
    public final String DOCUMENT_TYPE_NAME = "document";
    public final String DOCUMENT_META_FIELD_NAME = "__FOXTROT_METADATA__";
    public final String DOCUMENT_META_ID_FIELD_NAME = String.format("%s.id", DOCUMENT_META_FIELD_NAME);
    private final ElasticsearchConnection elasticsearchConnection;
    private final ElasticsearchConfig elasticsearchConfig;
    private final TableMetadataManager tableMetadataManager;

    public ElasticsearchIndexStoreManager(ElasticsearchConnection elasticsearchConnection,
                                          ElasticsearchConfig elasticsearchConfig,
                                          TableMetadataManager tableMetadataManager) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.elasticsearchConfig = elasticsearchConfig;
        this.tableMetadataManager = tableMetadataManager;
    }

    @Override
    public void initializeFoxtrot() throws StoreManagerException {
        PutIndexTemplateRequestBuilder builder = new PutIndexTemplateRequestBuilder(elasticsearchConnection.getClient().admin().indices(),
                "template_foxtrot_mappings");
        builder.setTemplate(String.format("%s-*", elasticsearchConfig.getTableNamePrefix()));
        try {
            builder.addMapping(DOCUMENT_TYPE_NAME, getDocumentMapping());
        } catch (IOException e) {
            throw new StoreManagerException(StoreManagerException.ErrorCode.INITIALIZATION_EXCEPTION, e.getMessage());
        }
        elasticsearchConnection.getClient().admin().indices().putTemplate(builder.request()).actionGet();
    }

    @Override
    public void initializeTable(String table) {
        // Nothing to be done here
    }

    @Override
    public void optimizeIndex() {

    }

    @Override
    public void cleanupIndexes() throws StoreManagerException {
        List<String> indicesToDelete = new ArrayList<String>();
        try {
            Set<String> currentIndices = elasticsearchConnection.getClient().admin().indices()
                    .prepareStatus().execute().actionGet().getIndices().keySet();
            indicesToDelete = decideIndicesToBeDeleted(currentIndices);
            logger.warn(String.format("Deleting Indexes - Indexes - %s", indicesToDelete));
            deleteIndices(indicesToDelete);
        } catch (Exception ex) {
            logger.error(String.format("Unable to delete Indexes - %s", indicesToDelete), ex);
            throw new StoreManagerException(StoreManagerException.ErrorCode.DATA_CLEANUP_EXCEPTION,
                    String.format("Unable to delete Indexes - %s", indicesToDelete), ex);
        }
    }

    private List<String> decideIndicesToBeDeleted(final Set<String> currentIndices) throws StoreManagerException {
        List<String> indicesToBeDeleted = new ArrayList<>();
        for (String currentIndex : currentIndices) {
            boolean indexEligibleForDeletion;
            indexEligibleForDeletion = isIndexEligibleForDeletion(currentIndex);
            if (indexEligibleForDeletion) {
                logger.warn(String.format("Index eligible for deletion : %s", currentIndex));
                indicesToBeDeleted.add(currentIndex);
            }
        }
        return indicesToBeDeleted;
    }

    private boolean isIndexEligibleForDeletion(final String index) throws StoreManagerException {
        if (index == null || index.trim().isEmpty()) {
            return false;
        }

        ElasticsearchParsedIndex parsedIndex = parseIndexName(index);
        if (parsedIndex != null) {
            DateTime startTime = new DateTime(0L);
            DateTime endTime;
            try {
                endTime = new DateTime().minusDays(tableMetadataManager.get(parsedIndex.getTable()).getTtl()).toDateMidnight().toDateTime();
            } catch (Exception e) {
                throw new StoreManagerException(StoreManagerException.ErrorCode.METADATA_FETCH_EXCEPTION, e);
            }
            return parsedIndex.getCreationDate().isAfter(startTime) && parsedIndex.getCreationDate().isBefore(endTime);
        }
        return false;
    }

    // Utility method for elasticsearch

    private ElasticsearchParsedIndex parseIndexName(final String index) {
        ElasticsearchParsedIndex parsedIndex = new ElasticsearchParsedIndex();
        // Parse table name
        if (index.startsWith(elasticsearchConfig.getTableNamePrefix()) && index.endsWith(TABLENAME_POSTFIX)) {
            String tempIndex = index.substring(elasticsearchConfig.getTableNamePrefix().length() + 1);
            int position = tempIndex.lastIndexOf(String.format("-%s", TABLENAME_POSTFIX));
            parsedIndex.setTable(tempIndex.substring(0, position));
        } else {
            return null;
        }

        // Parse creation date
        String indexPrefix = getIndexPrefix(parsedIndex.getTable());
        String creationDateString = index.substring(index.indexOf(indexPrefix) + indexPrefix.length());
        DateTime creationDate = DATE_TIME_FORMATTER.parseDateTime(creationDateString);
        parsedIndex.setCreationDate(creationDate);
        return parsedIndex;
    }

    private String getIndexPrefix(final String table) {
        return String.format("%s-%s-%s-", elasticsearchConfig.getTableNamePrefix(), table, TABLENAME_POSTFIX);
    }

    private void deleteIndices(final List<String> indicesToDelete) {
        if (indicesToDelete.size() > 0) {
            List<List<String>> subLists = Lists.partition(indicesToDelete, 5);
            for (List<String> subList : subLists) {
                try {
                    logger.warn(String.format("Deleting Indexes - %s", subList));
                    elasticsearchConnection.getClient().admin().indices()
                            .prepareDelete(subList.toArray(new String[subList.size()]))
                            .execute().actionGet(TimeValue.timeValueMinutes(5));
                    logger.warn(String.format("Deleted Indexes - %s", subList));
                } catch (Exception e) {
                    logger.error(String.format("Index deletion failed - %s", subList), e);
                }
            }
        }
    }

    public String getIndexNameForTimestamp(final String table, final long timestamp) {
        String datePostfix = DATE_TIME_FORMATTER.print(timestamp);
        return String.format("%s%s", getIndexPrefix(table), datePostfix);
    }

    public String getIndices(final String table) {
        return String.format("%s*", getIndexPrefix(table));
    }

    public String[] getIndices(final String table, final ActionRequest request) throws Exception {
        return getIndices(table, request, new PeriodSelector(request.getFilters()).analyze());
    }

    public String[] getIndices(final String table, final ActionRequest request, final Interval interval) throws Exception {
        DateTime start = interval.getStart().toLocalDate().toDateTimeAtStartOfDay();
        if (start.getYear() <= 1970) {
            logger.warn("Request of type {} running on all indices", request.getClass().getSimpleName());
            return new String[]{getIndices(table)};
        }
        List<String> indices = Lists.newArrayList();
        final DateTime end = interval.getEnd().plusDays(1).toLocalDate().toDateTimeAtStartOfDay();
        while (start.getMillis() < end.getMillis()) {
            final String index = getIndexNameForTimestamp(table, start.getMillis());
            indices.add(index);
            start = start.plusDays(1);
        }
        logger.info("Request of type {} on indices: {}", request.getClass().getSimpleName(), indices);
        return indices.toArray(new String[indices.size()]);
    }

    private XContentBuilder getDocumentMapping() throws IOException {
        return XContentFactory.jsonBuilder()
                .startObject()
                .field(DOCUMENT_TYPE_NAME)
                .startObject()
                .field("_source")
                .startObject()
                .field("enabled", false)
                .endObject()
                .field("_all")
                .startObject()
                .field("enabled", false)
                .endObject()
                .field("_timestamp")
                .startObject()
                .field("enabled", true)
                .field("store", true)
                .endObject()
                .field("dynamic_templates")
                .startArray()
                .startObject()
                .field("template_metadata_fields")
                .startObject()
                .field("path_match", DOCUMENT_META_FIELD_NAME + ".*")
                .field("mapping")
                .startObject()
                .field("store", true)
                .field("doc_values", true)
                .field("index", "not_analyzed")
                .field("fielddata")
                .startObject()
                .field("format", "doc_values")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .field("template_timestamp")
                .startObject()
                .field("match", "timestamp")
                .field("mapping")
                .startObject()
                .field("store", false)
                .field("index", "not_analyzed")
                .field("fielddata")
                .startObject()
                .field("format", "doc_values")
                .endObject()
                .field("type", "date")
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .field("template_no_store_analyzed")
                .startObject()
                .field("match", "*")
                .field("match_mapping_type", "string")
                .field("mapping")
                .startObject()
                .field("store", false)
                .field("index", "not_analyzed")
                .field("fielddata")
                .startObject()
                .field("format", "doc_values")
                .endObject()
                .field("fields")
                .startObject()
                .field("analyzed")
                .startObject()
                .field("store", false)
                .field("type", "string")
                .field("index", "analyzed")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .startObject()
                .field("template_no_store")
                .startObject()
                .field("match_mapping_type", "date|boolean|double|long|integer")
                .field("match_pattern", "regex")
                .field("path_match", ".*")
                .field("mapping")
                .startObject()
                .field("store", false)
                .field("index", "not_analyzed")
                .field("fielddata")
                .startObject()
                .field("format", "doc_values")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
                .endObject();
    }

    public String getValidTableName(String table) {
        if (table == null) return null;
        return table.toLowerCase();
    }
}
