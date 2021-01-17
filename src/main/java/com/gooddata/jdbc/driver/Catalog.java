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

/**
 * GoodData objects catalog. Includes both AFM (display form, metric) and LDM (attribute, fact, metric) objects
 */
public class Catalog {

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

    private final static Logger LOGGER = Logger.getLogger(Catalog.class.getName());

    /**
     * AFM objects (displayForms, and metrics)
     */
    private final Map<String, CatalogEntry> afmEntries = new HashMap<>();
    /**
     * LDM objects (facts, metrics and attributes)
     */
    private final Map<String, CatalogEntry> maqlEntries = new HashMap<>();

    /**
     * Constructor
     */
    public Catalog() {
    }

    public void addAttribute(Attribute a) throws SQLException {
        DisplayForm displayForm = a.getDefaultDisplayForm();
        CatalogEntry e = new CatalogEntry(displayForm.getUri(),
                a.getTitle(), displayForm.getCategory(), displayForm.getIdentifier(),
                new UriObjQualifier(displayForm.getUri()));
        //TODO getting default display form only
        // under the attribute title
        e.setDataType(CatalogEntry.DEFAULT_ATTRIBUTE_DATATYPE);
        this.afmEntries.put(displayForm.getUri(), e);
        this.maqlEntries.put(a.getUri(), new CatalogEntry(a.getUri(),
                a.getTitle(), a.getCategory(), a.getIdentifier(),
                new UriObjQualifier(a.getUri())));
    }

    public void addMetric(Entry metric) throws SQLException {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        this.afmEntries.put(metric.getUri(), e);
        this.maqlEntries.put(metric.getUri(), e);
    }

    public void addMetric(Metric metric) throws SQLException {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        this.afmEntries.put(metric.getUri(), e);
        this.maqlEntries.put(metric.getUri(), e);
    }


    public void addFact(Entry fact) {
        CatalogEntry e = new CatalogEntry(fact.getUri(),
                fact.getTitle(), fact.getCategory(), fact.getIdentifier(),
                new UriObjQualifier(fact.getUri()));
        this.maqlEntries.put(fact.getUri(), e);
    }

    /**
     * Populates the catalog of attributes and metrics
     *
     * @param gd        Gooddata reference
     * @param workspace workspace
     * @throws SQLException generic issue
     */
    public void populate(GoodData gd, Project workspace) throws SQLException {

        MetadataService gdMeta = gd.getMetadataService();
        Collection<Entry> metricEntries = gdMeta.find(workspace, Metric.class);

        for (Entry metric : metricEntries) {
            this.addMetric(metric);
        }

        Collection<Entry> attributeEntries = gdMeta.find(workspace, Attribute.class);
        for (Entry attribute : attributeEntries) {
            this.addAttribute(gdMeta.getObjByUri(attribute.getUri(), Attribute.class));
        }

        Collection<Entry> factEntries = gdMeta.find(workspace, Fact.class);
        for (Entry fact : factEntries) {
            this.addFact(fact);
        }
    }

    /**
     * Extracts schemas from the catalog objects
     *
     * @return all schemas extracted from all objects URIs
     * @throws SQLException in case of any issue
     */
    public List<String> getAllSchemas() throws SQLException {
        Set<String> schemas = new HashSet<>();
        Set<String> allObjects = new HashSet<>();
        allObjects.addAll(this.afmEntries.keySet());
        allObjects.addAll(this.maqlEntries.keySet());
        for (String uri : allObjects) {
            schemas.add(TextUtil.extractIdFromUri(uri));
        }
        return new ArrayList<>(schemas);
    }

    private Comparator<CatalogEntry> CatalogEntryComparator = Comparator.comparing(CatalogEntry::getTitle);

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
    public CatalogEntry findAfmColumnByTitle(String name) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException {
        List<CatalogEntry> objects = this.afmEntries.values().stream()
                .filter(catalogEntry -> name.equalsIgnoreCase(catalogEntry.getTitle())).collect(Collectors.toList());
        if (objects.size() > 1) {
            throw new DuplicateCatalogEntryException(
                    String.format("Column name '%s' can't be uniquely resolved. " +
                            "There are multiple LDM objects with this title.", name));
        } else if (objects.size() == 0) {
            throw new CatalogEntryNotFoundException(
                    String.format("Column name '%s' doesn't exist.", name));
        }
        return objects.get(0).cloneEntry();
    }

    /**
     * Finds LDM object (metric, fact, attribute) by title
     *
     * @param name object name
     * @return LDM object
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findMaqlColumnByTitle(String name) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException {
        List<CatalogEntry> objects = this.maqlEntries.values().stream()
                .filter(catalogEntry -> name.equalsIgnoreCase(catalogEntry.getTitle())).collect(Collectors.toList());
        if (objects.size() > 1) {
            throw new DuplicateCatalogEntryException(
                    String.format("Column name '%s' can't be uniquely resolved. " +
                            "There are multiple LDM objects with this title.", name));
        } else if (objects.size() == 0) {
            throw new CatalogEntryNotFoundException(
                    String.format("Column name '%s' doesn't exist.", name));
        }
        return objects.get(0).cloneEntry();
    }

    /**
     * Resolve the parsed SQL columns to AFM objects
     *
     * @param sql parsed SQL statement
     * @return list of AFM objects
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     * @throws SQLException                   generic problem
     */
    public List<CatalogEntry> resolveAfmColumns(SQLParser.ParsedSQL sql)
            throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, SQLException {

        List<CatalogEntry> c = new ArrayList<>();
        List<String> columns = sql.getColumns();
        for (String column : columns) {
            if (column.contains("::")) {
                String[] parsedColumn = Arrays.stream(column.split("::"))
                        .map(String::trim).toArray(String[]::new);
                if (parsedColumn.length != 2)
                    throw new CatalogEntryNotFoundException(String.format(
                            "Column '%s' doesn't exist.", column));
                CatalogEntry newColumn = findAfmColumnByTitle(parsedColumn[0]);
                newColumn.setDataType(parsedColumn[1]);
                c.add(newColumn);
            } else {
                CatalogEntry newColumn = findAfmColumnByTitle(column);
                if (newColumn.getType().equals("metric")) {
                    newColumn.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
                } else {
                    newColumn.setDataType(CatalogEntry.DEFAULT_ATTRIBUTE_DATATYPE);
                }
                c.add(newColumn);
            }
        }
        return c;
    }

    /**
     * Translates filter operator from parser to AFM
     *
     * @param parserOperator parser operator
     * @return AFM operator
     * @throws SQLException in case when unknown operator is supplied
     */
    private ComparisonConditionOperator getAfmComparisonOperatorFromParserOperator(int parserOperator) throws SQLException {
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
            CatalogEntryNotFoundException, SQLException {

        List<AfmFilter> afmFilters = new ArrayList<>();
        List<SQLParser.ParsedSQL.FilterExpression> sqlFilters = sql.getFilters();
        for (SQLParser.ParsedSQL.FilterExpression sqlFilter : sqlFilters) {
            String sqlFilterColumnName = sqlFilter.getColumn();
            CatalogEntry catalogEntry = findAfmColumnByTitle(sqlFilterColumnName);
            if (catalogEntry.getType().equalsIgnoreCase("metric")) {
                BigDecimal value = DataTypeParser.parseBigDecimal(sqlFilter.getValues().get(0));
                MeasureValueFilterCondition c = new ComparisonCondition(
                        getAfmComparisonOperatorFromParserOperator(sqlFilter.getOperator()),
                        value
                );
                CompatibilityFilter f = new MeasureValueFilter(catalogEntry.getGdObject(), c);
                afmFilters.add(new AfmFilter(catalogEntry, sqlFilter.getOperator(),
                        Collections.singletonList(value), f));
            } else {
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

}

