package com.gooddata.jdbc.catalog;

import com.gooddata.jdbc.parser.SQLParser;
import com.gooddata.sdk.model.executeafm.ObjQualifier;

import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * Catalog entry - holds LDM or AFM object
 */
public class CatalogEntry {

    private final static Logger LOGGER = Logger.getLogger(CatalogEntry.class.getName());
    public static String DEFAULT_ATTRIBUTE_DATATYPE = "VARCHAR(255)";
    public static String DEFAULT_METRIC_DATATYPE = "DECIMAL(13,2)";

    /**
     * Constructor
     *
     * @param uri        Catalog object URI
     * @param title      Catalog object title
     * @param type       Catalog object type
     * @param identifier Catalog object identifier
     */
    public CatalogEntry(String uri, String title, String type, String identifier, ObjQualifier gdObject) {
        this.uri = uri;
        this.title = title;
        this.type = type;
        this.identifier = identifier;
        this.gdObject = gdObject;
    }

    /**
     * Constructor
     * @param uri        Catalog object URI
     * @param title      Catalog object title
     * @param type       Catalog object type
     * @param identifier Catalog object identifier
     * @param gdObject   Original gd object
     * @param dataType   Datatype
     * @param size       Datatype size
     * @param precision  Datatype precision
     */
    public CatalogEntry(String uri, String title, String type, String identifier, ObjQualifier gdObject,
                        String dataType, int size, int precision) {
        this.identifier = identifier;
        this.uri = uri;
        this.title = title;
        this.type = type;
        this.dataType = dataType;
        this.size = size;
        this.precision = precision;
        this.gdObject = gdObject;
    }

    /**
     * Clone catalog object entry (when it needs to be modified externally)
     * @return catalog entry clone
     */
    public CatalogEntry cloneEntry() {
        return new CatalogEntry(this.uri, this.title, this.type, this.identifier, this.gdObject, this.dataType,
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

    public ObjQualifier getGdObject() {
        return gdObject;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        SQLParser.ParsedSQLDataType d = SQLParser.parseSqlDatatype(dataType);
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
    private final ObjQualifier gdObject;

}
