package com.gooddata.jdbc.catalog;

import com.gooddata.jdbc.parser.DataTypeParser;
import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.UriObjQualifier;
import com.gooddata.sdk.model.executeafm.afm.filter.*;
import com.gooddata.sdk.model.executeafm.result.Paging;
import com.gooddata.sdk.model.executeafm.resultspec.*;
import com.gooddata.sdk.model.md.*;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.md.MetadataService;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.w3c.dom.Text;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * GoodData objects catalog. Includes both AFM (display form, metric) and LDM (attribute, fact, metric) objects
 */
public class Catalog {

    private final static Logger LOGGER = Logger.getLogger(Catalog.class.getName());
    /**
     * AFM objects (displayForms, and metrics)
     */
    private final Map<String, CatalogEntry> afmEntries = new HashMap<>();
    /**
     * LDM objects (facts, metrics and attributes)
     */
    private final Map<String, CatalogEntry> maqlEntries = new HashMap<>();
    private final Comparator<CatalogEntry> CatalogEntryComparator = Comparator.comparing(CatalogEntry::getTitle);
    private final int[] ATTRIBUTE_FILTER_OPERATORS = new int[] {
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_IN,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_IN
    };
    private final int[] METRIC_FILTER_OPERATORS = new int[] {
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_GREATER,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_GREATER_OR_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_LOWER,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_LOWER_OR_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_BETWEEN,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_BETWEEN
    };

    // Is catalog populated?
    private boolean isCatalogPopulated = false;

    /**
     * Constructor
     */
    public Catalog() {
    }

    /**
     * Removes object from catalog
     * @param c object to remove
     */
    public void remove(CatalogEntry c) {
        this.afmEntries.remove(c.getUri());
        this.maqlEntries.remove(c.getUri());
    }

    /**
     * Adds attribute to catalog
     * @param attribute attribute to add
     */
    public void addAttribute(Attribute attribute) {
        LOGGER.info(String.format("Adding attribute title='%s'", attribute.getTitle()));
        if (attribute.getDisplayForms().size() > 0) {
            DisplayForm displayForm = attribute.getDefaultDisplayForm();
            LOGGER.info(String.format("Default display form title='%s'", displayForm.getTitle()));
            CatalogEntry e = new CatalogEntry(displayForm.getUri(),
                    attribute.getTitle(), displayForm.getCategory(), displayForm.getIdentifier(),
                    new UriObjQualifier(displayForm.getUri()));
            //TODO getting default display form only
            // under the attribute title
            e.setDataType(CatalogEntry.DEFAULT_ATTRIBUTE_DATATYPE);
            this.afmEntries.put(displayForm.getUri(), e);
            this.maqlEntries.put(attribute.getUri(), new CatalogEntry(attribute.getUri(),
                    attribute.getTitle(), attribute.getCategory(), attribute.getIdentifier(),
                    new UriObjQualifier(attribute.getUri())));
        }
        else {
            LOGGER.info(String.format("Skipping attribute title='%s'", attribute.getTitle()));
        }
    }

    /**
     * Adds metric to catalog
     * @param metric metric to add
     */
    public void addMetric(Entry metric) {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        this.afmEntries.put(metric.getUri(), e);
        this.maqlEntries.put(metric.getUri(), e);
    }

    /**
     * Adds metric to catalog
     * @param metric metric to add
     */
    public void addMetric(Metric metric) {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        this.afmEntries.put(metric.getUri(), e);
        this.maqlEntries.put(metric.getUri(), e);
    }

    /**
     * Adds fact to catalog
     * @param fact metric to add
     */
    public void addFact(Entry fact) {
        CatalogEntry e = new CatalogEntry(fact.getUri(),
                fact.getTitle(), fact.getCategory(), fact.getIdentifier(),
                new UriObjQualifier(fact.getUri()));
        this.maqlEntries.put(fact.getUri(), e);
    }

    private class PopulateExecutor extends Thread {

        private GoodData gd;
        private String workspaceUri;

        public PopulateExecutor(GoodData gd, String workspaceUri) {
            this.gd = gd;
            this.workspaceUri = workspaceUri;
        }

        @Override
        public void run() {
            try {
                populateSync(this.gd, this.workspaceUri);
            } catch (SQLException e) {
                // TBD better error handling
                System.err.println(e);
            }
        }
    }

    public synchronized void waitForCatalogPopulationFinished() {
        while (!this.isCatalogPopulated) {
            try {
                wait();
            } catch (InterruptedException e) {
                // TBD better handling
                e.printStackTrace();
            }
        }
    }

    /**
     * Populates the catalog of attributes and metrics
     *
     * @param gd        Gooddata reference
     * @param workspaceUri GoodData workspace URI
     * @throws SQLException generic issue
     */
    public void populateAsync(GoodData gd, String workspaceUri) throws SQLException {
        PopulateExecutor exec = new PopulateExecutor(gd, workspaceUri);
        exec.start();
    }

    /**
     * Populates the catalog of attributes and metrics
     *
     * @param gd        Gooddata reference
     * @param workspaceUri GoodData workspace URI
     * @throws SQLException generic issue
     */
    public synchronized void populateSync(GoodData gd, String workspaceUri) throws SQLException {
        LOGGER.info(String.format("Populating catalog for schema '%s'", workspaceUri));
        LOGGER.info("Catalog lock acquired.");
        this.isCatalogPopulated = false;
        Project workspace = gd.getProjectService().getProjectByUri(workspaceUri);
        if (workspace == null) {
            throw new SQLException(String.format("Workspace '%s' doesn't exist.", workspaceUri));
        }
        MetadataService gdMeta = gd.getMetadataService();
        LOGGER.info("Fetching metrics.");
        Collection<Entry> metricEntries = gdMeta.find(workspace, Metric.class);

        for (Entry metric : metricEntries) {
            this.addMetric(metric);
        }

        LOGGER.info("Fetching attributes.");
        Collection<Entry> attributeEntries = gdMeta.find(workspace, Attribute.class);
        for (Entry attribute : attributeEntries) {
            this.addAttribute(gdMeta.getObjByUri(attribute.getUri(), Attribute.class));
        }

        LOGGER.info("Fetching facts.");
        Collection<Entry> factEntries = gdMeta.find(workspace, Fact.class);
        for (Entry fact : factEntries) {
            this.addFact(fact);
        }
        this.isCatalogPopulated = true;
        LOGGER.info(String.format("Catalog population finished. Fetched '%d' AFM and '%d' objects.",
                this.afmEntries.size(), this.maqlEntries.size()));
        notifyAll();
        LOGGER.info("Catalog lock released");
    }

    /**
     * Get all AFM entries
     *
     * @return AFM objects collection
     */
    public Collection<CatalogEntry> afmEntries() {
        return this.afmEntries.values().stream().sorted(CatalogEntryComparator)
                .collect(Collectors.toList());
    }

    /**
     * Get all LDM entries
     *
     * @return LDM objects collection
     */
    public Collection<CatalogEntry> maqlEntries() {
        return this.maqlEntries.values().stream().sorted(CatalogEntryComparator)
                .collect(Collectors.toList());
    }

    /**
     * Finds AFM object by title
     *
     * @param name AFM object name
     * @return the AFM object
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findAfmColumn(String name) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        return this.findColumn(name, this.afmEntries);
    }

    /**
     * Finds MAQL object by title
     *
     * @param name MAQL object name
     * @return the MAQL object
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findMaqlColumn(String name) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        return this.findColumn(name, this.maqlEntries);
    }

    /**
     * Finds catalog object (metric, fact, attribute, display form)
     *
     * @param name object name
     * @param catalog MAQL or AFM object catalog
     * @return LDM object
     * @throws DuplicateCatalogEntryException in case when there are multiple catalog objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findColumn(String name, Map<String,CatalogEntry> catalog) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        if(TextUtil.isGoodDataColumnWithUri(name)) {
            String uri = TextUtil.extractGoodDataUriFromColumnName(name);
            CatalogEntry c = catalog.get(uri);
            if(c != null) {
                return c.cloneEntry();
            } else {
                throw new CatalogEntryNotFoundException(String.format("Catalog object with uri '%s' not found.", name));
            }
        } else {
            List<CatalogEntry> objects = catalog.values().stream()
                    .filter(catalogEntry -> name.equalsIgnoreCase(catalogEntry.getTitle())).collect(Collectors.toList());
            if (objects.size() > 1) {
                throw new DuplicateCatalogEntryException(
                        String.format("Column name '%s' can't be uniquely resolved. " +
                                "There are multiple catalog objects with this title.", name));
            } else if (objects.size() == 0) {
                throw new CatalogEntryNotFoundException(
                        String.format("Column name '%s' doesn't exist.", name));
            }
            return objects.get(0).cloneEntry();
        }
    }

    /**
     * Resolve the parsed SQL columns to AFM objects
     *
     * @param sql parsed SQL statement
     * @return list of AFM objects
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     * @throws TextUtil.InvalidFormatException  generic problem
     */
    public List<CatalogEntry> resolveAfmColumns(SQLParser.ParsedSQL sql)
            throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        List<CatalogEntry> c = new ArrayList<>();
        List<String> columns = sql.getColumns();
        for (String column : columns) {
            SQLParser.ParsedColumnName parsedColumn = SQLParser.parseColumnWithDataTypeSpecifier(column);
            CatalogEntry newColumn = findAfmColumn(parsedColumn.getName());
            if(parsedColumn.getDatatype() != null) {
                newColumn.setDataType(parsedColumn.getDatatype());
            }
            else {
                if (newColumn.getType().equals("metric")) {
                    newColumn.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
                } else {
                    newColumn.setDataType(CatalogEntry.DEFAULT_ATTRIBUTE_DATATYPE);
                }
            }
            c.add(newColumn);
        }
        return c;
    }

    public List<SortItem> resolveOrderBys(SQLParser.ParsedSQL parsedSql,
                                          List<CatalogEntry> columns) throws SQLException {
        List<SQLParser.ParsedSQL.OrderByExpression> orderBys = parsedSql.getOrderBys();
        List<SortItem> sortItems = new ArrayList<>();
        for(SQLParser.ParsedSQL.OrderByExpression orderByElement: orderBys) {
            CatalogEntry c;
            String orderColumn = orderByElement.getColumn();
            try {
                int order = Integer.parseInt(orderColumn);
                if(order <= 0 || order > columns.size())
                    throw new SQLException(String.format("ORDER BY column '%s' is too high.", orderColumn));
                c = columns.get(order - 1);
            }
            catch (NumberFormatException e) {
                List<CatalogEntry> l;
                try {
                    if (TextUtil.isGoodDataColumnWithUri(orderColumn)) {
                        String uri = TextUtil.extractGoodDataUriFromColumnName(orderColumn);
                        l = columns.stream().filter(i -> i.getUri().equals(uri)).collect(Collectors.toList());
                    }
                    else {
                        l = columns.stream().filter(i -> i.getTitle().equals(orderColumn)).collect(Collectors.toList());
                    }
                    if(l.size() > 1 || l.size() <= 0) {
                        throw new SQLException(String.format("Can't uniquely resolve the ORDER BY column '%s'",
                                orderColumn));
                    }
                    c = l.get(0);
                } catch (TextUtil.InvalidFormatException e1) {
                    throw new SQLException(e1);
                }
            }
            if(c.getType().equals("metric")) {
                sortItems.add(new MeasureSortItem(orderByElement.getOrder().toLowerCase(),
                        Arrays.asList(new MeasureLocatorItem(c.getIdentifier()))));
            }
            else {
                sortItems.add(new AttributeSortItem(orderByElement.getOrder().toLowerCase(),
                        c.getIdentifier()));
            }
        }
        return sortItems;
    }

    /**
     * Translates filter operator from parser to AFM
     *
     * @param parserOperator parser operator
     * @return AFM operator
     * @throws SQLException in case when unknown operator is supplied
     */
    private ComparisonConditionOperator getAfmComparisonOperatorFromParserOperator(int parserOperator)
            throws SQLException {
        switch (parserOperator) {
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
                "Unsupported filter operator '%d'", parserOperator));
    }

    /**
     * Resolve the parsed SQL filters to AFM filters
     *
     * @param sql parsed SQL statement
     * @return list of AFM filters
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     * @throws SQLException                   generic problem
     */
    public List<AfmFilter> resolveAfmFilters(SQLParser.ParsedSQL sql)
            throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, SQLException, TextUtil.InvalidFormatException {

        List<AfmFilter> afmFilters = new ArrayList<>();
        List<SQLParser.ParsedSQL.FilterExpression> sqlFilters = sql.getFilters();
        for (SQLParser.ParsedSQL.FilterExpression sqlFilter : sqlFilters) {
            String sqlFilterColumnName = sqlFilter.getColumn();
            CatalogEntry catalogEntry = findAfmColumn(sqlFilterColumnName);

            if (catalogEntry.getType().equalsIgnoreCase("metric")) {
                if(!ArrayUtils.contains(METRIC_FILTER_OPERATORS, sqlFilter.getOperator()))
                        throw new SQLException("Only =,<>,>=,<=,>,<,BETWEEN, and NOT BETWEEN " +
                                "operators are supported for metrics.");
                if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_BETWEEN ||
                        sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_BETWEEN) {
                    BigDecimal valueStart = DataTypeParser.parseBigDecimal(sqlFilter.getValues().get(0));
                    BigDecimal valueEnd = DataTypeParser.parseBigDecimal(sqlFilter.getValues().get(1));
                    RangeCondition c = new RangeCondition(
                            sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_BETWEEN
                                    ? RangeConditionOperator.BETWEEN
                                    : RangeConditionOperator.NOT_BETWEEN
                            , valueStart, valueEnd);
                    List<Object> values = Arrays.asList(valueStart, valueEnd);
                    CompatibilityFilter f = new MeasureValueFilter(catalogEntry.getGdObject(), c);
                    afmFilters.add(new AfmFilter(catalogEntry, sqlFilter.getOperator(), values, f));
                } else {
                    BigDecimal value = DataTypeParser.parseBigDecimal(sqlFilter.getValues().get(0));
                    MeasureValueFilterCondition c = new ComparisonCondition(
                            getAfmComparisonOperatorFromParserOperator(sqlFilter.getOperator()),
                            value
                    );
                    CompatibilityFilter f = new MeasureValueFilter(catalogEntry.getGdObject(), c);
                    List<Object> values = Collections.singletonList(value);
                    afmFilters.add(new AfmFilter(catalogEntry, sqlFilter.getOperator(), values, f));
                }

            } else {
                if(!ArrayUtils.contains(ATTRIBUTE_FILTER_OPERATORS, sqlFilter.getOperator()))
                    throw new SQLException("Only =,<>,IN, and NOT IN " +
                            "operators are supported for attributes.");
                CompatibilityFilter f;
                List<String> values = sqlFilter.getValues();
                ValueAttributeFilterElements e = new ValueAttributeFilterElements(values);
                if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL) {
                    f = new PositiveAttributeFilter(catalogEntry.getGdObject(), e);
                } else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL) {
                    f = new NegativeAttributeFilter(catalogEntry.getGdObject(), e);
                } else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_IN) {
                    f = new PositiveAttributeFilter(catalogEntry.getGdObject(), e);
                }  else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_IN) {
                    f = new NegativeAttributeFilter(catalogEntry.getGdObject(), e);
                }
                else {
                    throw new SQLException(String.format(
                            "Unsupported attribute filter operator '%d'", sqlFilter.getOperator()));
                }
                afmFilters.add(new AfmFilter(catalogEntry, sqlFilter.getOperator(), Collections.singletonList(values), f));
            }
        }
        return afmFilters;
    }

    /**
     * Duplicate LDM object exception is thrown when there are multiple LDM objects with the same title
     */
    public static class DuplicateCatalogEntryException extends Exception {
        public DuplicateCatalogEntryException(String e) {
            super(e);
        }
    }

    /**
     * Thrown when a LDM object with a title isn't found
     */
    public static class CatalogEntryNotFoundException extends Exception {
        public CatalogEntryNotFoundException(String e) {
            super(e);
        }
        public CatalogEntryNotFoundException(Exception e) {
            super(e);
        }
    }

}

