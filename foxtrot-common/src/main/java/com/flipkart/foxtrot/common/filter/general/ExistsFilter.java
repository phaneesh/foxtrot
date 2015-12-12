package com.flipkart.foxtrot.common.filter.general;

import com.flipkart.foxtrot.common.filter.Filter;
import com.flipkart.foxtrot.common.filter.FilterOperator;
import com.flipkart.foxtrot.common.filter.FilterVisitor;

/**
 * Created by rishabh.goyal on 03/11/14.
 */
public class ExistsFilter extends Filter {

    public ExistsFilter() {
        super(FilterOperator.exists);
    }

    public ExistsFilter(String field) {
        super(FilterOperator.exists, field);
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }
}
