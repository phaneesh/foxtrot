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
package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.common.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.querystore.QueryStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
public class NonCacheableActionTest extends ActionTest {

    @Before
    public void setUp() throws Exception {
        super.setUp();
        queryStore.save(TestUtils.TEST_TABLE_NAME, TestUtils.getQueryDocuments(mapper));
    }

    @After
    public void tearDown() throws IOException {
        elasticsearchServer.shutdown();
        hazelcastInstance.shutdown();
    }

    //TODO how to verify if cache is hit or not ?
    @Test
    public void checkCacheability() throws QueryStoreException {
        queryExecutor.execute(new NonCacheableActionRequest());
    }
}
