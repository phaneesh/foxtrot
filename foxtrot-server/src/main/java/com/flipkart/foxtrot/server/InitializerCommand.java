/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.manager.impl.ElasticsearchIndexStoreManager;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializerCommand extends ConfiguredCommand<FoxtrotServerConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(InitializerCommand.class.getSimpleName());

    public InitializerCommand() {
        super("initialize", "Initialize elasticsearch and hbase");
    }

    @Override
    protected void run(Bootstrap<FoxtrotServerConfiguration> bootstrap,
                       Namespace namespace,
                       FoxtrotServerConfiguration configuration) throws Exception {
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(configuration.getElasticsearch());
        HazelcastConnection hazelcastConnection = new HazelcastConnection(configuration.getCluster(), new ObjectMapper());
        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection,
                elasticsearchConnection);
        ElasticsearchIndexStoreManager indexStoreManager = new ElasticsearchIndexStoreManager(elasticsearchConnection,
                configuration.getElasticsearch(), tableMetadataManager);
        indexStoreManager.initializeFoxtrot();

        ClusterHealthResponse clusterHealth = new ClusterHealthRequestBuilder(elasticsearchConnection.getClient().admin().cluster())
                .execute()
                .get();

        int numDataNodes = clusterHealth.getNumberOfDataNodes();
        int numReplicas = (numDataNodes < 2) ? 0 : 1;

        logger.info("# data nodes: {}, Setting replica count to: {}", numDataNodes, numReplicas);

        createMetaIndex(elasticsearchConnection, "consoles", numDataNodes - 1);
        createMetaIndex(elasticsearchConnection, "table-meta", numDataNodes - 1);

        logger.info("Creating hbase table");
        HBaseUtil.createTable(configuration.getHbase(), configuration.getHbase().getTableName());
    }

    private void createMetaIndex(final ElasticsearchConnection connection,
                                 final String indexName,
                                 int replicaCount) throws Exception {
        try {
            CreateIndexResponse response = new CreateIndexRequestBuilder(connection.getClient().admin().indices(), indexName)
                    .setSettings(
                            ImmutableSettings.builder()
                                    .put("number_of_shards", 1)
                                    .put("number_of_replicas", replicaCount)
                    )
                    .execute()
                    .get();
            logger.info("'{}' creation acknowledged: {}", indexName, response.isAcknowledged());
            if (!response.isAcknowledged()) {
                logger.error("Index {} could not be created.", indexName);
            }
        } catch (Exception e) {
            if (null != e.getCause()) {
                logger.error("Index {} could not be created: {}", indexName, e.getCause().getLocalizedMessage());
            } else {
                logger.error("Index {} could not be created: {}", indexName, e.getLocalizedMessage());
            }
        }
    }


}
