package com.flipkart.foxtrot.common.stats;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.filter.FilterCombiner;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * Created by rishabh.goyal on 02/08/14.
 */
public class StatsRequest extends ActionRequest {

    @NotNull
    @NotEmpty
    private String table;

    @NotNull
    @NotEmpty
    private String field;

    @NotNull
    private FilterCombiner combiner = FilterCombiner.and;

    public StatsRequest() {

    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public FilterCombiner getCombiner() {
        return combiner;
    }

    public void setCombiner(FilterCombiner combiner) {
        this.combiner = combiner;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("table", table)
                .append("field", field)
                .append("filters", getFilters())
                .append("combiner", combiner)
                .toString();
    }
}
