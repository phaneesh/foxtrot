package com.flipkart.foxtrot.core.manager.impl;

import org.joda.time.DateTime;

/**
 * Created by rishabh.goyal on 09/12/15.
 */
public class ElasticsearchParsedIndex {

    private String table;
    private DateTime creationDate;

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public DateTime getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(DateTime creationDate) {
        this.creationDate = creationDate;
    }
}
