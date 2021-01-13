package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.DataTypeParser;
import com.gooddata.sdk.model.executeafm.ObjQualifier;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AfmColumn {

    private final static Logger LOGGER = Logger.getLogger(AfmColumn.class.getName());
    public static String DEFAULT_ATTRIBUTE_DATATYPE = "VARCHAR(255)";
    public static String DEFAULT_METRIC_DATATYPE = "DECIMAL(13,2)";

    /**
     * Constructor
     *
     * @param uri        LDM object URI
     * @param title      LDM object title
     * @param type       LDM object type
     * @param identifier LDM object identifier
     */
    public AfmColumn(String uri, String title, String type, String identifier, ObjQualifier ldmObject) {
        this.uri = uri;
        this.title = title;
        this.type = type;
        this.identifier = identifier;
        this.ldmObject = ldmObject;
    }

    public AfmColumn(String uri, String title, String type, String identifier, ObjQualifier ldmObject,
                     String dataType, int size, int precision) {
        this.identifier = identifier;
        this.uri = uri;
        this.title = title;
        this.type = type;
        this.dataType = dataType;
        this.size = size;
        this.precision = precision;
        this.ldmObject = ldmObject;
    }

    public AfmColumn cloneEntry() {
        return new AfmColumn(this.uri, this.title, this.type, this.identifier, this.ldmObject, this.dataType,
                this.size, this.precision);
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public ObjQualifier getLdmObject() {
        return ldmObject;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) throws SQLException {
        DataTypeParser.ParsedSQLDataType d = DataTypeParser.parseSqlDatatype(dataType);
        this.dataType = d.getName();
        this.setSize(d.getSize());
        this.setPrecision(d.getPrecision());
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    private String identifier;
    private String uri;
    private String title;
    private String type;
    private String dataType;
    private int size;
    private int precision;
    private final ObjQualifier ldmObject;


    /**
     * Duplicate LDM object exception is thrown when there are multiple LDM objects with the same title
     */
    public static class DuplicateLdmObjectException extends Exception {
        public DuplicateLdmObjectException(String e) {
            super(e);
        }
    }

    /**
     * Thrown when a LDM object with a title isn't found
     */
    public static class LdmObjectNotFoundException extends Exception {
        public LdmObjectNotFoundException(String e) {
            super(e);
        }
    }

}
