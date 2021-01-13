package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.DataTypeParser;
import com.gooddata.sdk.model.executeafm.ObjQualifier;
import com.gooddata.sdk.model.executeafm.afm.filter.CompatibilityFilter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class AfmFilter {

    private final static Logger LOGGER = Logger.getLogger(AfmFilter.class.getName());

    public AfmFilter(AfmColumn column, int operator, List<Object> values, CompatibilityFilter filterObj) {
        this.column = column;
        this.operator = operator;
        this.values = values;
        this.filterObj = filterObj;
    }

    public AfmColumn getColumn() {
        return column;
    }

    public void setColumn(AfmColumn column) {
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

    private AfmColumn column;
    private int operator;
    private List<Object> values;
    private CompatibilityFilter filterObj;

}
