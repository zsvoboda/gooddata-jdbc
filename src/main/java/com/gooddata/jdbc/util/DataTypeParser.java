package com.gooddata.jdbc.util;

import com.sun.istack.NotNull;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.sql.Types;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataTypeParser {

    private final static Logger LOGGER = Logger.getLogger(DataTypeParser.class.getName());

    private static boolean containsIgnoreCase(List<String> l, String s) {
        return l.stream().anyMatch(s::equalsIgnoreCase);
    }

    private static final List<String> FALSE_VALUES = Arrays.asList("0", "false", "f");
    private static final List<String> TRUE_VALUES = Arrays.asList("1", "true", "t");

    /**
     * Return boolean value
     *
     * @param textValue text value
     * @return boolean value
     * @throws SQLException in case when the value cannot be converted
     */
    public static boolean parseBoolean(@NotNull String textValue) throws SQLException {
        if (textValue == null) return false;
        if (containsIgnoreCase(FALSE_VALUES, textValue)) {
            return false;
        }
        if (containsIgnoreCase(TRUE_VALUES, textValue)) {
            return true;
        }
        throw new SQLException(String.format("The value '%s' can't be converted to boolean.", textValue));
    }

    /**
     * Return short value
     *
     * @param textValue text value
     * @return short value
     * @throws SQLException in case when the value cannot be converted
     */
    public static short parseShort(@NotNull String textValue) throws SQLException {
        if (textValue == null) return 0;
        try {
            return Short.parseShort(textValue);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return int value
     *
     * @param textValue text value
     * @return int value
     * @throws SQLException in case when the value cannot be converted
     */
    public static int parseInt(@NotNull String textValue) throws SQLException {
        if (textValue == null) return 0;
        try {
            return Integer.parseInt(textValue);
        } catch (NumberFormatException e) {
            try {
                return parseInt(textValue.split("\\.")[0]);
            } catch(NumberFormatException e1) {
                throw new SQLException(e1);
            }
        }
    }

    /**
     * Return long value
     *
     * @param textValue text value
     * @return long value
     * @throws SQLException in case when the value cannot be converted
     */
    public static long parseLong(@NotNull String textValue) throws SQLException {
        if (textValue == null) return 0;
        try {
            return Long.parseLong(textValue);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return float value
     *
     * @param textValue text value
     * @return float value
     * @throws SQLException in case when the value cannot be converted
     */
    public static float parseFloat(@NotNull String textValue) throws SQLException {
        if (textValue == null) return 0;
        try {
            return Float.parseFloat(textValue);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return double value
     *
     * @param textValue text value
     * @return double value
     * @throws SQLException in case when the value cannot be converted
     */
    public static double parseDouble(@NotNull String textValue) throws SQLException {
        if (textValue == null) return 0;
        try {
            return Double.parseDouble(textValue);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return BigDecimal value
     *
     * @param textValue text value
     * @return BigDecimal value
     * @throws SQLException in case when the value cannot be converted
     */
    public static BigDecimal parseBigDecimal(@NotNull String textValue) throws SQLException {
        if (textValue == null) return null;
        try {
            return new BigDecimal(textValue);
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return BigDecimal value
     *
     * @param textValue text value
     * @param precision     precision
     * @return BigDecimal value
     * @throws SQLException in case when the value cannot be converted
     */
    public static BigDecimal parseBigDecimal(@NotNull String textValue, int precision) throws SQLException {
        if (textValue == null) return null;
        try {
            return new BigDecimal(textValue, new MathContext(precision));
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return Object value
     *
     * @param textValue text value
     * @return object value
     * @throws SQLException in case when the value cannot be converted
     */
    public static Object parseObject(@NotNull String textValue, int sqlType,
                                     int precision) throws SQLException {
        if (textValue == null) return null;
            switch(sqlType) {
                case Types.NUMERIC:
                case Types.DECIMAL:
                    return parseBigDecimal(textValue, precision);
                case Types.VARCHAR:
                case Types.CHAR:
                    return textValue;
                case Types.DOUBLE:
                    return parseDouble(textValue);
                case Types.FLOAT:
                    return parseFloat(textValue);
                case Types.INTEGER:
                    return parseInt(textValue);
            }
            throw new SQLException(String.format(
                    "Unsupported java.sql.Types type '%d'", sqlType));
    }

    public static int convertSQLDataTypeNameToJavaSQLType(String sqlTypeName)  {
        if(sqlTypeName.equalsIgnoreCase("VARCHAR"))
            return Types.VARCHAR;
        if(sqlTypeName.equalsIgnoreCase("NUMERIC"))
            return Types.NUMERIC;
        if(sqlTypeName.equalsIgnoreCase("DECIMAL"))
            return Types.DECIMAL;
        if(sqlTypeName.equalsIgnoreCase("DOUBLE"))
            return Types.DOUBLE;
        if(sqlTypeName.equalsIgnoreCase("FLOAT"))
            return Types.FLOAT;
        if(sqlTypeName.equalsIgnoreCase("INTEGER"))
            return Types.INTEGER;
        if(sqlTypeName.equalsIgnoreCase("CHAR"))
            return Types.CHAR;
        if(sqlTypeName.equalsIgnoreCase("DATE"))
            return Types.DATE;
        if(sqlTypeName.equalsIgnoreCase("TIME"))
            return Types.TIME;
        if(sqlTypeName.equalsIgnoreCase("DATETIME") || sqlTypeName.equalsIgnoreCase("TIMESTAMP"))
            return Types.TIMESTAMP;
        throw new RuntimeException(String.format("Data type '%s' is not supported.", sqlTypeName));
    }

    public static String convertSQLDataTypeNameToJavaClassName(String sqlTypeName)  {
        if(sqlTypeName.equalsIgnoreCase("VARCHAR"))
            return "java.lang.String";
        if(sqlTypeName.equalsIgnoreCase("NUMERIC"))
            return "java.math.BigDecimal";
        if(sqlTypeName.equalsIgnoreCase("DECIMAL"))
            return "java.math.BigDecimal";
        if(sqlTypeName.equalsIgnoreCase("DOUBLE"))
            return "java.lang.Double";
        if(sqlTypeName.equalsIgnoreCase("FLOAT"))
            return "java.lang.Float";
        if(sqlTypeName.equalsIgnoreCase("INTEGER"))
            return "java.lang.Integer";
        if(sqlTypeName.equalsIgnoreCase("CHAR"))
            return "java.lang.String";
        if(sqlTypeName.equalsIgnoreCase("DATE"))
            return "java.sql.Date";
        if(sqlTypeName.equalsIgnoreCase("TIME"))
            return "java.sql.Time";
        if(sqlTypeName.equalsIgnoreCase("DATETIME") || sqlTypeName.equalsIgnoreCase("TIMESTAMP"))
            return "java.sql.Timestamp";
        throw new RuntimeException(String.format("Data type '%s' is not supported.", sqlTypeName));
    }

    public static class ParsedSQLDataType {

        public ParsedSQLDataType(String name, int size, int precision) {
            this.name = name;
            this.size = size;
            this.precision = precision;
        }

        public ParsedSQLDataType(String name) {
            this.name = name;
        }

        public ParsedSQLDataType(String name, int size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
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

        private String name;
        private int size;
        private int precision;
    }

    public static ParsedSQLDataType parseSqlDatatype(String dataType) throws SQLException {
        String dataTypeName;
        int size = 0;
        int precision = 0;
        Pattern p1 = Pattern.compile("^\\s?([a-zA-Z]+)\\s?(\\(\\s?([0-9]+)\\s?(\\s?,\\s?([0-9]+)\\s?)?\\s?\\))?\\s?$");
        Matcher m1 = p1.matcher(dataType);
        m1.matches();
        int cnt = m1.groupCount();
        dataTypeName = m1.group(1);
        String sizeTxt = m1.group(3);
        if(sizeTxt!=null)
            size = Integer.parseInt(sizeTxt);
        String precisionTxt = m1.group(5);
        if(precisionTxt!=null)
            precision = Integer.parseInt(precisionTxt);
        return new ParsedSQLDataType(dataTypeName, size, precision);
    }


}
