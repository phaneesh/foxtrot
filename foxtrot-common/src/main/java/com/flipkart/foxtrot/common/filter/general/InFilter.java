package com.flipkart.foxtrot.common.filter.general;

import com.flipkart.foxtrot.common.filter.Filter;
import com.flipkart.foxtrot.common.filter.FilterOperator;
import com.flipkart.foxtrot.common.filter.FilterVisitor;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: rishabh.goyal
 * Date: 02/09/14
 * Time: 11:46 AM
 * To change this template use File | Settings | File Templates.
 */
public class InFilter extends Filter {

    @NotNull
    private List<Object> values;

    public InFilter() {
        super(FilterOperator.in);
    }

    public InFilter(String field, List<Object> values) {
        super(FilterOperator.in, field);
        this.values = values;
    }

    @Override
    public void accept(FilterVisitor visitor) throws Exception {
        visitor.visit(this);
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        InFilter inFilter = (InFilter) o;

        if (values != null ? !values.equals(inFilter.values) : inFilter.values != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (values != null ? values.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .appendSuper(super.toString())
                .append("values", values)
                .toString();
    }
}
