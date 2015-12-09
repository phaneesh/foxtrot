/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core;

import org.apache.commons.io.FileUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by rishabh.goyal on 16/04/14.
 */

public class MockElasticsearchServer {
    private final Node node;
    private String dataDirectory;

    public MockElasticsearchServer(String directory) {
        this.dataDirectory = String.format("target/%s", UUID.randomUUID().toString());
        ImmutableSettings.Builder elasticsearchSettings = ImmutableSettings.settingsBuilder()
                .put("http.enabled", "false")
                .put("path.data", dataDirectory);
        node = nodeBuilder()
                .local(true)
                .clusterName(UUID.randomUUID().toString())
                .settings(elasticsearchSettings.build())
                .node();
    }

    public void refresh(final String index) {
        node.client().admin().indices().refresh(new RefreshRequest().indices(index)).actionGet();
    }

    public Client getClient() {
        return node.client();
    }

    public void shutdown() throws IOException {
        node.client().admin().indices().delete(new DeleteIndexRequest("table-meta")
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, true))).actionGet();
        node.close();
        while (!node.isClosed()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        deleteDataDirectory();
    }

    private void deleteDataDirectory() throws IOException {
        System.out.println("Deleting DATA DIRECTORY " + dataDirectory);
        FileUtils.deleteDirectory(new File(dataDirectory));
    }
}