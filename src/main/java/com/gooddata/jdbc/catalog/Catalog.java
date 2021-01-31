package com.gooddata.jdbc.catalog;

import com.gooddata.jdbc.parser.DataTypeParser;
import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.jdbc.rest.GoodDataRestConnection;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.UriObjQualifier;
import com.gooddata.sdk.model.executeafm.afm.filter.*;
import com.gooddata.sdk.model.executeafm.resultspec.AttributeSortItem;
import com.gooddata.sdk.model.executeafm.resultspec.MeasureLocatorItem;
import com.gooddata.sdk.model.executeafm.resultspec.MeasureSortItem;
import com.gooddata.sdk.model.executeafm.resultspec.SortItem;
import com.gooddata.sdk.model.md.*;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.md.MetadataService;
import org.apache.commons.lang3.ArrayUtils;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * GoodData objects catalog. Includes both AFM (display form, metric) and LDM (attribute, fact, metric) objects
 */
public class Catalog implements Serializable {

    private final static Logger LOGGER = Logger.getLogger(Catalog.class.getName());

    /**
     * AFM objects (displayForms, and metrics)
     */
    private Map<String, CatalogEntry> entries = new HashMap<>();

    private final Comparator<CatalogEntry> CatalogEntryComparator = Comparator.comparing(CatalogEntry::getTitle);
    private final int[] ATTRIBUTE_FILTER_OPERATORS = new int[]{
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_IN,
            SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_IN
    };

    private final int[] METRIC_FILTER_OPERATORS = new int[]{
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
    private boolean isCatalogPopulated = true;

    /**
     * Constructor
     */
    public Catalog(GoodData gd, GoodDataRestConnection gdRest, Schema schema) throws SQLException {
        try {
            LOGGER.info(String.format("Trying to deserialize catalog for workspace '%s'.",
                    schema.getSchemaUri()));
           this.entries = this.deserialize(TextUtil
                    .extractWorkspaceIdFromWorkspaceUri(schema.getSchemaUri()));
            LOGGER.info("Catalog successfully  deserialized.");
        } catch (IOException e) {
            LOGGER.info("Catalog deserialization failed. Creating new one.");
            this.populate(gd, gdRest, schema.getSchemaUri());
        } catch (ClassNotFoundException | TextUtil.InvalidFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Removes object from catalog
     *
     * @param c object to remove
     */
    public void removeEntry(CatalogEntry c) {
        this.entries.remove(c.getUri());
    }

    /**
     * Adds attribute to catalog
     *
     * @param attribute attribute to add
     */
    private void addAttribute(Attribute attribute, Map<String, CatalogEntry> c) {
        LOGGER.info(String.format("Adding attribute title='%s'", attribute.getTitle()));
        if (attribute.getDisplayForms().size() > 0) {
            DisplayForm displayForm = attribute.getDefaultDisplayForm();
            LOGGER.info(String.format("Default display form title='%s'", displayForm.getTitle()));
            CatalogEntry e = new CatalogEntry(attribute.getUri(),
                    attribute.getTitle(), attribute.getCategory(), attribute.getIdentifier(),
                    new UriObjQualifier(attribute.getUri()),  new UriObjQualifier(displayForm.getUri()));
            //TODO getting default display form only
            e.setDataType(CatalogEntry.DEFAULT_ATTRIBUTE_DATATYPE);
            c.put(attribute.getUri(), e);
        } else {
            LOGGER.info(String.format("Skipping attribute title='%s'", attribute.getTitle()));
        }
    }


    /**
     * Adds metric to catalog
     *
     * @param metric metric to add
     */
    private void addMetric(Entry metric, Map<String,CatalogEntry> c) {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        c.put(metric.getUri(), e);
    }

    /**
     * Adds metric to catalog
     *
     * @param metric metric to add
     */
    public void addMetric(Metric metric) {
        CatalogEntry e = new CatalogEntry(metric.getUri(),
                metric.getTitle(), metric.getCategory(), metric.getIdentifier(),
                new UriObjQualifier(metric.getUri()));
        e.setDataType(CatalogEntry.DEFAULT_METRIC_DATATYPE);
        this.entries.put(metric.getUri(), e);
    }

    /**
     * Adds fact to catalog
     *
     * @param fact metric to add
     */
    private void addFact(Entry fact, Map<String, CatalogEntry> c) {
        CatalogEntry e = new CatalogEntry(fact.getUri(),
                fact.getTitle(), fact.getCategory(), fact.getIdentifier(),
                new UriObjQualifier(fact.getUri()));
        c.put(fact.getUri(), e);
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
     * @param gd           Gooddata reference
     * @param gdRest       Gooddata REST connection
     * @param workspaceUri GoodData workspace URI
     *
     */
    public void populate(GoodData gd, GoodDataRestConnection gdRest, String workspaceUri) {
        CatalogRefreshExecutor exec = new CatalogRefreshExecutor(this, gd, gdRest, workspaceUri);
        exec.start();
    }

    /**
     * Populates the catalog of attributes and metrics
     *
     * @param gd           Gooddata reference
     * @param gdRest       Gooddata REST connection
     * @param workspaceUri GoodData workspace URI
     * @throws SQLException generic issue
     */
    public synchronized void populateSync(GoodData gd, GoodDataRestConnection gdRest, String workspaceUri) throws SQLException {
        LOGGER.info(String.format("Populating catalog for schema '%s'", workspaceUri));
        if(this.entries.size() == 0) {
            // Must block all queries if the catalog is empty
            LOGGER.info("Catalog lock acquired.");
            this.isCatalogPopulated = false;
        } else {
            // if there is something in the catalog, the queries can use the old content
            this.isCatalogPopulated = true;
        }
        this.entries = this.populateCatalogEntries(gd, gdRest, workspaceUri);
        this.isCatalogPopulated = true;
        notifyAll();
        LOGGER.info("Catalog lock released");
    }

    /**
     * Populate new entries map
     *
     * @param gd           Gooddata reference
     * @param gdRest       Gooddata REST connection
     * @param workspaceUri GoodData workspace URI
     * @return new entries Map
     * @throws SQLException generic issue
     */
    private Map<String,CatalogEntry> populateCatalogEntries(GoodData gd, GoodDataRestConnection gdRest, String workspaceUri) throws SQLException {
        LOGGER.info(String.format("Refreshing catalog for workspace uri '%s'", workspaceUri));
        Map<String,CatalogEntry> newCatalog = new HashMap<>();

        Project workspace = gd.getProjectService().getProjectByUri(workspaceUri);
        if (workspace == null) {
            throw new SQLException(String.format("Workspace '%s' doesn't exist.", workspaceUri));
        }
        MetadataService gdMeta = gd.getMetadataService();
        LOGGER.info("Fetching metrics.");
        Collection<Entry> metricEntries = gdMeta.find(workspace, Metric.class);

        for (Entry metric : metricEntries) {
            this.addMetric(metric, newCatalog);
        }

        LOGGER.info("Fetching attributes.");
        Collection<Entry> attributeEntries = gdMeta.find(workspace, Attribute.class);
        for (Entry attribute : attributeEntries) {
            this.addAttribute(gdMeta.getObjByUri(attribute.getUri(), Attribute.class), newCatalog);
        }

        LOGGER.info("Fetching facts.");
        Collection<Entry> factEntries = gdMeta.find(workspace, Fact.class);
        for (Entry fact : factEntries) {
            this.addFact(fact, newCatalog);
        }

        LOGGER.info("Fetching variables.");
        List<CatalogEntry> variableEntries = gdRest.getVariables(workspaceUri);
        for (CatalogEntry variable : variableEntries) {
            newCatalog.put(variable.getUri(), variable);
        }
        LOGGER.info(String.format("Catalog refresh finished. Fetched '%d' objects.", newCatalog.size()));

        LOGGER.info(String.format("Serializing catalog for workspace '%s'.", workspaceUri));
        try {
            this.serialize(TextUtil.extractWorkspaceIdFromWorkspaceUri(workspaceUri), newCatalog);
        } catch (TextUtil.InvalidFormatException | IOException e) {
            throw new SQLException(e);
        }
        LOGGER.info(String.format("Catalog serialization finished for workspace '%s'.", workspaceUri));
        return newCatalog;
    }

    private static final String SERIALIZATION_DIR = ".gdjdbc";
    private static final String SERIALIZATION_EXTENSION = ".gdcat";

    private void ensureSerializationDirectory() throws IOException {
        Files.createDirectories(Paths.get(String.format("%s/%s",
                System.getProperty("user.home"), SERIALIZATION_DIR)));
    }

    private void serialize(String schema, Map<String, CatalogEntry> c) throws IOException {
        LOGGER.info(String.format("Serializing catalog '%s'", schema));
        ensureSerializationDirectory();

        FileOutputStream fileOut = new FileOutputStream(String.format("%s/%s/%s.%s",
                System.getProperty("user.home"), SERIALIZATION_DIR, schema, SERIALIZATION_EXTENSION));
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(c);
        out.close();
        fileOut.close();

        LOGGER.info(String.format("Catalog '%s' serialized successfully.", schema));
    }

    private Map<String, CatalogEntry> deserialize(String schema) throws IOException, ClassNotFoundException {
        LOGGER.info(String.format("Deserializing catalog '%s'", schema));
        ensureSerializationDirectory();
        FileInputStream fileIn = new FileInputStream(String.format("%s/%s/%s.%s",
                System.getProperty("user.home"), SERIALIZATION_DIR, schema, SERIALIZATION_EXTENSION));
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Object o = in.readObject();
        in.close();
        fileIn.close();
        LOGGER.info(String.format("Catalog '%s' deserialized successfully.", schema));
        return (Map<String, CatalogEntry>)o;
    }

    /**
     * Get all entries
     *
     * @return objects collection
     */
    public Collection<CatalogEntry> entries() {
        return this.entries.values().stream().sorted(CatalogEntryComparator)
                .collect(Collectors.toList());
    }

    public CatalogEntry get(String uri) {
        return this.entries.get(uri);
    }

    /**
     * Finds object by title
     *
     * @param name AFM object name
     * @return the AFM object
     * @throws DuplicateCatalogEntryException in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findByName(String name) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        return this.findColumn(name, this.entries);
    }

    /**
     * Finds catalog object (metric, fact, attribute, display form)
     *
     * @param name    object name
     * @param catalog MAQL or AFM object catalog
     * @return LDM object
     * @throws DuplicateCatalogEntryException in case when there are multiple catalog objects with the same title
     * @throws CatalogEntryNotFoundException  in case when a matching object doesn't exist
     */
    public CatalogEntry findColumn(String name, Map<String, CatalogEntry> catalog) throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        if (TextUtil.isGoodDataColumnWithUri(name)) {
            String uri = TextUtil.extractGoodDataUriFromColumnName(name);
            CatalogEntry c = catalog.get(uri);
            if (c != null) {
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
     * @throws DuplicateCatalogEntryException  in case when there are multiple AFM objects with the same title
     * @throws CatalogEntryNotFoundException   in case when a matching object doesn't exist
     * @throws TextUtil.InvalidFormatException generic problem
     */
    public List<CatalogEntry> resolveAfmColumns(SQLParser.ParsedSQL sql)
            throws DuplicateCatalogEntryException,
            CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        List<CatalogEntry> c = new ArrayList<>();
        List<String> columns = sql.getColumns();
        for (String column : columns) {
            SQLParser.ParsedColumnName parsedColumn = SQLParser.parseColumnWithDataTypeSpecifier(column);
            CatalogEntry newColumn = findByName(parsedColumn.getName());
            if (parsedColumn.getDatatype() != null) {
                newColumn.setDataType(parsedColumn.getDatatype());
            } else {
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
        for (SQLParser.ParsedSQL.OrderByExpression orderByElement : orderBys) {
            CatalogEntry c;
            String orderColumn = orderByElement.getColumn();
            try {
                int order = Integer.parseInt(orderColumn);
                if (order <= 0 || order > columns.size())
                    throw new SQLException(String.format("ORDER BY column '%s' is too high.", orderColumn));
                c = columns.get(order - 1);
            } catch (NumberFormatException e) {
                List<CatalogEntry> l;
                try {
                    if (TextUtil.isGoodDataColumnWithUri(orderColumn)) {
                        String uri = TextUtil.extractGoodDataUriFromColumnName(orderColumn);
                        l = columns.stream().filter(i -> i.getUri().equals(uri)).collect(Collectors.toList());
                    } else {
                        l = columns.stream().filter(i -> i.getTitle().equals(orderColumn)).collect(Collectors.toList());
                    }
                    if (l.size() != 1) {
                        throw new SQLException(String.format("Can't uniquely resolve the ORDER BY column '%s'",
                                orderColumn));
                    }
                    c = l.get(0);
                } catch (TextUtil.InvalidFormatException e1) {
                    throw new SQLException(e1);
                }
            }
            if (c.getType().equals("metric")) {
                sortItems.add(new MeasureSortItem(orderByElement.getOrder().toLowerCase(),
                        Collections.singletonList(new MeasureLocatorItem(c.getIdentifier()))));
            } else {
                sortItems.add(new AttributeSortItem(orderByElement.getOrder().toLowerCase(),
                        c.getDefaultDisplayForm().getUri()));
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
            CatalogEntry catalogEntry = findByName(sqlFilterColumnName);

            if (catalogEntry.getType().equalsIgnoreCase("metric")) {
                if (!ArrayUtils.contains(METRIC_FILTER_OPERATORS, sqlFilter.getOperator()))
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
                if (!ArrayUtils.contains(ATTRIBUTE_FILTER_OPERATORS, sqlFilter.getOperator()))
                    throw new SQLException("Only =,<>,IN, and NOT IN " +
                            "operators are supported for attributes.");
                CompatibilityFilter f;
                List<String> quotedValues = sqlFilter.getValues();
                List<String> unQuotedValues = quotedValues.stream().filter(e -> !(e.startsWith("'") && e.endsWith("'")))
                        .collect(Collectors.toList());

                if (unQuotedValues.size() > 0) {
                    throw new SQLException(String.format("WHERE condition attribute values without quotes '%s'",
                            unQuotedValues));
                }
                List<String> values = sqlFilter.getValues().stream().map(e -> e.replace("'", ""))
                        .collect(Collectors.toList());
                ValueAttributeFilterElements e = new ValueAttributeFilterElements(values);
                if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL) {
                    f = new PositiveAttributeFilter(catalogEntry.getDefaultDisplayForm(), e);
                } else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL) {
                    f = new NegativeAttributeFilter(catalogEntry.getDefaultDisplayForm(), e);
                } else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_IN) {
                    f = new PositiveAttributeFilter(catalogEntry.getDefaultDisplayForm(), e);
                } else if (sqlFilter.getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_IN) {
                    f = new NegativeAttributeFilter(catalogEntry.getDefaultDisplayForm(), e);
                } else {
                    throw new SQLException(String.format(
                            "Unsupported attribute filter operator '%d'", sqlFilter.getOperator()));
                }
                afmFilters.add(new AfmFilter(catalogEntry, sqlFilter.getOperator(), Collections.singletonList(values), f));
            }
        }
        return afmFilters;
    }

    /**
     * Prints metric with substituted uris for names
     *
     * @param gdMeta GD metadata service
     * @param gdRest GD REST connection
     * @param uri    metric uri
     * @return metric MAQL with resolved URIs
     */
    public String getMetricPrettyPrint(MetadataService gdMeta, GoodDataRestConnection gdRest, String uri)
            throws CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        Metric m = gdMeta.getObjByUri(uri, Metric.class);
        if (!m.getCategory().equalsIgnoreCase("metric")) {
            throw new CatalogEntryNotFoundException(String.format("Metric with uri '%s' not found.", uri));
        }
        String e = m.getExpression();
        e = substituteUris(gdRest, e);
        return e;
    }

    /**
     * Prints variable with substituted uris for names
     *
     * @param gdRest GD REST connection
     * @param uri    metric uri
     * @return metric MAQL with resolved URIs
     */
    public String getVariablePrettyPrint(GoodDataRestConnection gdRest, String uri)
            throws CatalogEntryNotFoundException, TextUtil.InvalidFormatException {
        CatalogEntry e = this.entries.get(uri);
        if (!e.getType().equalsIgnoreCase("prompt")) {
            throw new CatalogEntryNotFoundException(String.format("Variable with uri '%s' not found.", uri));
        }
        GoodDataRestConnection.Variable v = (GoodDataRestConnection.Variable) e.getGdObject();
        return substituteUris(gdRest, v.getExpression());
    }

    /**
     * Substitute URIs for names
     *
     * @param gdRest GD REST connection
     * @param e      String with the URIs
     * @return String with URIs replaced with names
     * @throws TextUtil.InvalidFormatException invalid URI format
     * @throws CatalogEntryNotFoundException   non-existent catalog entries
     */
    private String substituteUris(GoodDataRestConnection gdRest, String e) throws TextUtil.InvalidFormatException,
            CatalogEntryNotFoundException {
        List<String> objUris = TextUtil.findAllObjectUris(e);
        for (String objUri : objUris) {
            CatalogEntry obj = this.entries.get(objUri);
            if (obj != null)
                e = e.replace(String.format("[%s]", objUri), String.format("\"%s\"", obj.getTitle()));
        }
        List<String> elementUris = TextUtil.findAllElementUris(e);
        for (String elementUri : elementUris) {
            // The attribute element URI has ID of attribute but can be only looked up via display form
            // We must switch the URI part from attribute uri to display form uri
            String[] components = elementUri.split("\\?");
            if (components.length != 2) {
                throw new CatalogEntryNotFoundException(String.format("Invalid attribute element uri format '%s'",
                        elementUri));
            }
            CatalogEntry attribute = this.entries.get(components[0].replace("/elements", ""));
            if (attribute == null) {
                throw new CatalogEntryNotFoundException(String.format("Attribute with uri '%s' not found.",
                        elementUri));
            }
            String displayFormUri = attribute.getDefaultDisplayForm().getUri();
            if (displayFormUri == null) {
                throw new CatalogEntryNotFoundException(String.format("Attribute with uri '%s' doesn't have default display form.",
                        elementUri));
            }
            String text = gdRest.getAttributeElementText(String.format("%s/elements?%s", displayFormUri, components[1]));
            if (text != null)
                e = e.replace(String.format("[%s]", elementUri), String.format("'%s'", text));
        }
        return e;
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

