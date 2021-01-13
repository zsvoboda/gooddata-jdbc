package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.DataTypeParser;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.UriObjQualifier;
import com.gooddata.sdk.model.executeafm.afm.filter.*;
import com.gooddata.sdk.model.md.*;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.md.MetadataService;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AfmCatalog {

    private final static Logger LOGGER = Logger.getLogger(AfmCatalog.class.getName());

    /**
     * Catalog of LDM objects (attributes and metrics)
     */
    private final Map<String, AfmColumn> catalog = new HashMap<>();

    public AfmCatalog() {
    }

    /**
     * Populates the catalog of attributes and metrics
     */
    public void populate(GoodData gd, Project workspace) throws SQLException {

        MetadataService gdMeta = gd.getMetadataService();

        Collection<Entry> metricEntries = gdMeta.find(workspace, Metric.class);

        for(Entry metric: metricEntries) {
            AfmColumn e = new AfmColumn(metric.getUri(),
                    metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                    new UriObjQualifier(metric.getUri()));
            e.setDataType(AfmColumn.DEFAULT_METRIC_DATATYPE);
            this.catalog.put(metric.getUri(), e);
        }

        Collection<Entry> attributeEntries = gdMeta.find(workspace, Attribute.class);
        for(Entry attribute: attributeEntries) {
            Attribute a = gdMeta.getObjByUri(attribute.getUri(), Attribute.class);
            DisplayForm displayForm = a.getDefaultDisplayForm();
            AfmColumn e = new AfmColumn(displayForm.getUri(),
                    a.getTitle(), displayForm.getCategory(), displayForm.getIdentifier(),
                    new UriObjQualifier(displayForm.getUri()));
            //TODO getting default display form under the attribute title
            e.setDataType(AfmColumn.DEFAULT_ATTRIBUTE_DATATYPE);
            this.catalog.put(displayForm.getUri(), e);
        }
    }

    public List<String> getAllSchemas() throws SQLException {
        Set<String> schemas = new HashSet<>();
        for(String uri: this.catalog.keySet()) {
            schemas.add(TextUtil.extractWorkspaceIdFromUri(uri));
        }
        return new ArrayList<>(schemas);
    }

    public Collection<AfmColumn> entries() {
        return this.catalog.values();
    }

    public AfmColumn findAfmColumnByTitle(String name) throws AfmColumn.DuplicateLdmObjectException,
            AfmColumn.LdmObjectNotFoundException {
        List<AfmColumn> objects = this.catalog.values().stream()
                .filter(catalogEntry -> name.equalsIgnoreCase(catalogEntry.getTitle())).collect(Collectors.toList());
        if(objects.size() > 1) {
            throw new AfmColumn.DuplicateLdmObjectException(
                    String.format("Column name '%s' can't be uniquely resolved. " +
                            "There are multiple LDM objects with this title.", name));
        } else if(objects.size() == 0) {
            throw new AfmColumn.LdmObjectNotFoundException(
                    String.format("Column name '%s' doesn't exist.", name));
        }
        return objects.get(0).cloneEntry();
    }


    public  List<AfmColumn> resolveAfmColumns(SQLParser.ParsedSQL sql)
            throws AfmColumn.DuplicateLdmObjectException,
            AfmColumn.LdmObjectNotFoundException, SQLException {

        List<AfmColumn> c = new ArrayList<>();
        List<String> columns = sql.getColumns();
        for(String column: columns ) {
            if(column.contains("::")) {
                String[] parsedColumn = Arrays.stream(column.split("::"))
                        .map(s->s.trim()).toArray(String[]::new);
                if(parsedColumn == null || parsedColumn.length != 2)
                    throw new AfmColumn.LdmObjectNotFoundException(String.format("Column '%s' doesn't exist.", column));
                AfmColumn newColumn = findAfmColumnByTitle(parsedColumn[0]);
                newColumn.setDataType(parsedColumn[1]);
                c.add(newColumn);
            }
            else {
                AfmColumn newColumn = findAfmColumnByTitle(column);
                if(newColumn.getType().equals("metric")) {
                    newColumn.setDataType(AfmColumn.DEFAULT_METRIC_DATATYPE);
                }
                else {
                    newColumn.setDataType(AfmColumn.DEFAULT_ATTRIBUTE_DATATYPE);
                }
                c.add(newColumn);
            }
        }
        return c;
    }

    private ComparisonConditionOperator getAfmComparisonOperator(int jdbcOperator) throws SQLException {
        switch(jdbcOperator) {
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL:
                return ComparisonConditionOperator.EQUAL_TO;
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL:
                return ComparisonConditionOperator.NOT_EQUAL_TO;
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_GREATER:
                return ComparisonConditionOperator.GREATER_THAN;
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_GREATER_OR_EQUAL:
                return ComparisonConditionOperator.GREATER_THAN_OR_EQUAL_TO;
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_LOWER:
                return ComparisonConditionOperator.LESS_THAN;
            case SQLParser.ParsedSQL.FilterExpression.OPERATOR_LOWER_OR_EQUAL:
                return ComparisonConditionOperator.LESS_THAN_OR_EQUAL_TO;
        }
        throw new SQLException(String.format(
                "Unsupported filter operator '%d'", jdbcOperator));
    }

    public List<AfmFilter> resolveAfmFilters(SQLParser.ParsedSQL sql)
            throws AfmColumn.DuplicateLdmObjectException,
            AfmColumn.LdmObjectNotFoundException, SQLException {

        List<AfmFilter> afmFilters = new ArrayList<>();
        List<SQLParser.ParsedSQL.FilterExpression> sqlFilters = sql.getFilters();
        for(SQLParser.ParsedSQL.FilterExpression sqlFilter: sqlFilters) {
            String sqlFilterColumnName = sqlFilter.getColumn();
            AfmColumn afmColumn = findAfmColumnByTitle(sqlFilterColumnName);
            if(afmColumn.getType().equalsIgnoreCase("metric")) {
                BigDecimal value = DataTypeParser.parseBigDecimal(sqlFilter.getValues().get(0));
                MeasureValueFilterCondition c = new ComparisonCondition(
                        getAfmComparisonOperator(sqlFilter.getOperator()),
                        value
                );
                CompatibilityFilter f = new MeasureValueFilter( afmColumn.getLdmObject(), c);
                afmFilters.add(new AfmFilter(afmColumn, sqlFilter.getOperator(),
                        Arrays.asList(value), f));
            }
            else {
                CompatibilityFilter f;
                List<String> values = sqlFilter.getValues();
                ValueAttributeFilterElements e = new ValueAttributeFilterElements(values);
                if(sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL) {
                    f = new PositiveAttributeFilter(afmColumn.getLdmObject(), e);
                } else if(sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL) {
                    f = new NegativeAttributeFilter(afmColumn.getLdmObject(), e);
                } else {
                    throw new SQLException(String.format(
                            "Unsupported attribute filter operator '%d'", sqlFilter.getOperator()));
                }
                afmFilters.add(new AfmFilter(afmColumn, sqlFilter.getOperator(), Arrays.asList(values), f));
            }
        }
        return afmFilters;
    }

}
