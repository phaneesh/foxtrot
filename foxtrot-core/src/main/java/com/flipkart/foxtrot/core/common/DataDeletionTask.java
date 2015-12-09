package com.flipkart.foxtrot.core.common;

import com.flipkart.foxtrot.core.manager.IndexStoreManager;
import com.flipkart.foxtrot.core.manager.StoreManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * Created by rishabh.goyal on 07/07/14.
 */
public class DataDeletionTask extends TimerTask {
    private static final Logger logger = LoggerFactory.getLogger(DataDeletionTask.class);
    private final IndexStoreManager indexStoreManager;

    public DataDeletionTask(IndexStoreManager indexStoreManager) {
        this.indexStoreManager = indexStoreManager;
    }

    @Override
    public void run() {
        logger.info("Starting Deletion Job");
        try {
            indexStoreManager.cleanupIndexes();
        } catch (StoreManagerException e) {
            logger.error("Deletion Job Failed ", e);
        }
        logger.info("Finished Deletion Job");
    }
}
