package com.flipkart.foxtrot.common.filter.general;

import com.flipkart.foxtrot.common.filter.Filter;
import com.flipkart.foxtrot.common.filter.FilterOperator;
import com.flipkart.foxtrot.common.filter.FilterVisitor;

/**
 * Created by avanish.pandey on 23/11/15.
 */
public class MissingFilter extends Filter{


	public MissingFilter() {
		super(FilterOperator.missing);
	}

	public MissingFilter(String field) {
		super(FilterOperator.missing, field);
	}

	@Override
	public void accept(FilterVisitor visitor) throws Exception {
		visitor.visit(this);

	}


}
