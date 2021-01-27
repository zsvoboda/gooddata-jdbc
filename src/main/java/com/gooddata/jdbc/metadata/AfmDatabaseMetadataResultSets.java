package com.gooddata.jdbc.metadata;

import com.gooddata.jdbc.catalog.Catalog;
import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.catalog.Schema;
import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.jdbc.resultset.MetadataResultSet;
import com.gooddata.sdk.model.project.Project;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Database metadata ResultSet
 */
public class AfmDatabaseMetadataResultSets {

    /**
     * Catalog ResultSet
     * @return ResultSet with catalog metadata
     */
    static MetadataResultSet catalogResultSet() {
        return new MetadataResultSet(
                Collections.singletonList(
                        new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                                Collections.singletonList(""))
                )
        );
    }

    /**
     * Table type ResultSet
     * @return ResultSet with table type metadata
     */
    static MetadataResultSet tableTypeResultSet() {
        return new MetadataResultSet(
                Collections.singletonList(
                        new MetadataResultSet.MetaDataColumn("TABLE_TYPE",
                                Arrays.asList("GLOBAL TEMPORARY", "LOCAL TEMPORARY",
                                        "SYSTEM TABLE", "TABLE", "VIEW"))
                ));
    }

    /**
     * Empty ResultSet
     * @return ResultSet with no metadata
     */
    static MetadataResultSet emptyResultSet() {
        List<String> empty = new ArrayList<>();
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("1",
                        empty),
                new MetadataResultSet.MetaDataColumn("2",
                        empty),
                new MetadataResultSet.MetaDataColumn("3",
                        empty),
                new MetadataResultSet.MetaDataColumn("4",
                        empty),
                new MetadataResultSet.MetaDataColumn("5",
                        empty),
                new MetadataResultSet.MetaDataColumn("6",
                        empty),
                new MetadataResultSet.MetaDataColumn("7",
                        empty),
                new MetadataResultSet.MetaDataColumn("8",
                        empty),
                new MetadataResultSet.MetaDataColumn("9",
                        empty),
                new MetadataResultSet.MetaDataColumn("10",
                        empty)
        );
        return new MetadataResultSet(data);
    }

        /**
     * Schema ResultSet
     * @param schemas GoodData schemas (workspaces)
     * @return ResultSet with schema metadata
     */
    static MetadataResultSet schemaResultSet(List<Schema> schemas, String schemaPattern) {
        List<Schema> filteredSchemas = schemas.stream()
                .filter(e->e.getSchemaName().equals(schemaPattern))
                .collect(Collectors.toList());
        return schemaResultSet(filteredSchemas);
    }

    /**
     * Schema ResultSet
     * @param schemas GoodData schemas (workspaces)
     * @return ResultSet with schema metadata
     */
    static MetadataResultSet schemaResultSet(List<Schema> schemas) {
            List<String> catalogs = schemas.stream()
                    .map(e -> "").collect(Collectors.toList());
            List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                    new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                            schemas.stream().map(Schema::getSchemaName).collect(Collectors.toList())),
                    new MetadataResultSet.MetaDataColumn("TABLE_CATALOG",
                            catalogs)
            );
            return new MetadataResultSet(data);
    }

    /**
     * Column ResultSet
     * @param catalog GoodData objects catalog
     * @param workspace GoodData project
     * @return ResultSet with column metadata
     */
    static MetadataResultSet columnResultSet(Catalog catalog, Project workspace) {

        List<String> columns = catalog.afmEntries().stream()
                .map(CatalogEntry::getTitle).collect(Collectors.toList());
        List<String> nil = columns.stream()
                .map(e -> (String) null)
                .collect(Collectors.toList());
        List<String> empty = columns.stream()
                .map(e -> "")
                .collect(Collectors.toList());
        List<String> ordinal = IntStream.range(1, columns.size() + 1)
                .boxed().map(e -> Integer.toString(e)).collect(Collectors.toList());

        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                        columns.stream()
                                .map(e -> "")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        columns.stream()
                                .map(e -> workspace.getTitle())
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TABLE_NAME",
                        columns.stream()
                                .map(e -> AfmResultSetMetaData.UNIVERSAL_TABLE_NAME)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("COLUMN_NAME",
                        columns),
                new MetadataResultSet.MetaDataColumn("DATA_TYPE", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> Integer.toString(
                                        SQLParser.convertSQLDataTypeNameToJavaSQLType(e.getDataType())))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("TYPE_NAME",
                        catalog.afmEntries().stream()
                                .map(CatalogEntry::getDataType)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("COLUMN_SIZE", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> Integer.toString(e.getSize()))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("BUFFER_LENGTH", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> Integer.toString(e.getSize() + e.getPrecision()))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("DECIMAL_DIGITS", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> Integer.toString(e.getPrecision()))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("NUM_PREC_RADIX", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> e.getType().equalsIgnoreCase("metric")
                                        ? "10" : null)
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("NULLABLE", "INTEGER",
                        columns.stream()
                                .map(e -> "1")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("REMARKS", nil),
                new MetadataResultSet.MetaDataColumn("COLUMN_DEF", empty),
                new MetadataResultSet.MetaDataColumn("SQL_DATA_TYPE", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> Integer.toString(
                                        SQLParser.convertSQLDataTypeNameToJavaSQLType(e.getDataType())))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("SQL_DATETIME_SUB", "INTEGER", nil),
                new MetadataResultSet.MetaDataColumn("CHAR_OCTET_LENGTH", "INTEGER",
                        catalog.afmEntries().stream()
                                .map(e -> e.getType().equals("metric") ?
                                        null
                                        : Integer.toString(e.getSize()))
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("ORDINAL_POSITION", "INTEGER", ordinal),
                new MetadataResultSet.MetaDataColumn("IS_NULLABLE",
                        columns.stream()
                                .map(e -> "YES")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("SCOPE_CATALOG", nil),
                new MetadataResultSet.MetaDataColumn("SCOPE_SCHEMA", nil),
                new MetadataResultSet.MetaDataColumn("SCOPE_TABLE", nil),
                new MetadataResultSet.MetaDataColumn("SOURCE_DATA_TYPE", nil),
                new MetadataResultSet.MetaDataColumn("IS_AUTOINCREMENT",
                        columns.stream()
                                .map(e -> "NO")
                                .collect(Collectors.toList())),
                new MetadataResultSet.MetaDataColumn("IS_GENERATEDCOLUMN",
                        columns.stream()
                                .map(e -> "NO")
                                .collect(Collectors.toList()))
        );
        return new MetadataResultSet(data);
    }

    /**
     * Column ResultSet
     * @param workspace GoodData project
     * @return ResultSet with column metadata
     */
    static MetadataResultSet tableResultSet(Project workspace) {
        List<String> empty = Collections.nCopies(1, null);
        List<MetadataResultSet.MetaDataColumn> data = Arrays.asList(
                new MetadataResultSet.MetaDataColumn("TABLE_CAT",
                        Collections.singletonList("")),
                new MetadataResultSet.MetaDataColumn("TABLE_SCHEM",
                        Collections.singletonList(workspace.getTitle())),
                new MetadataResultSet.MetaDataColumn("TABLE_NAME",
                        Collections.singletonList(AfmResultSetMetaData.UNIVERSAL_TABLE_NAME)),
                new MetadataResultSet.MetaDataColumn("TABLE_TYPE",
                        Collections.singletonList("TABLE")),
                new MetadataResultSet.MetaDataColumn("REMARKS", empty)
                ,
                new MetadataResultSet.MetaDataColumn("TYPE_CAT",
                        empty),
                new MetadataResultSet.MetaDataColumn("TYPE_SCHEM",
                        empty),
                new MetadataResultSet.MetaDataColumn("TYPE_NAME",
                        empty),
                new MetadataResultSet.MetaDataColumn("SELF_REFERENCING_COL_NAME",
                        empty),
                new MetadataResultSet.MetaDataColumn("REF_GENERATION",
                        empty)
        );
        return new MetadataResultSet(data);
    }


}
