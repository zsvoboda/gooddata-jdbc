package com.gooddata.jdbc.driver;

import com.gooddata.sdk.model.executeafm.afm.filter.CompatibilityFilter;

import java.util.List;
import java.util.logging.Logger;

/**
 * Represents AFM filter
 */
public class AfmFilter {

    private final static Logger LOGGER = Logger.getLogger(AfmFilter.class.getName());

    /**
     * Constructor
     * @param column filter column
     * @param operator filter operator
     * @param values filter values
     * @param filterObj new filter object
     */
    public AfmFilter(CatalogEntry column, int operator, List<Object> values, CompatibilityFilter filterObj) {
        this.column = column;
        this.operator = operator;
        this.values = values;
        this.filterObj = filterObj;
    }

    public CatalogEntry getColumn() {
        return column;
    }

    public void setColumn(CatalogEntry column) {
        this.column = column;
    }

    public int getOperator() {
        return operator;
    }

    public void setOperator(int operator) {
        this.operator = operator;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    public CompatibilityFilter getFilterObj() {
        return filterObj;
    }

    public void setFilterObj(CompatibilityFilter filterObj) {
        this.filterObj = filterObj;
    }

    private CatalogEntry column;
    private int operator;
    private List<Object> values;
    private CompatibilityFilter filterObj;

}
