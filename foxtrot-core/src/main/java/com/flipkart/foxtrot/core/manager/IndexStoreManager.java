package com.flipkart.foxtrot.core.manager;

/**
 * Created by rishabh.goyal on 09/12/15.
 */
public interface IndexStoreManager {

    void initializeFoxtrot() throws StoreManagerException;

    void initializeTable(String table) throws StoreManagerException;

    void optimizeIndex() throws StoreManagerException;

    void cleanupIndexes() throws StoreManagerException;

}
