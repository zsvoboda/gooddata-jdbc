package com.gooddata.jdbc.util;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

public class DataTypeParser {

    private static boolean containsIgnoreCase(List<String> l, String s) {
        return l.stream().anyMatch(s::equalsIgnoreCase);
    }
    private static final List<String> FALSE_VALUES = Arrays.asList("0", "false", "f");
    private static final List<String> TRUE_VALUES = Arrays.asList("1", "true", "t");

    /**
     * Return boolean value
     * @param textValue text value
     * @return boolean value
     * @throws SQLException in case when the value cannot be converted
     */
    public static boolean parseBoolean(String textValue) throws SQLException {
        if(containsIgnoreCase(FALSE_VALUES, textValue)) {
            return false;
        }
        if(containsIgnoreCase(TRUE_VALUES, textValue)) {
            return true;
        }
        throw new SQLException(String.format("The value '%s' can't be converted to boolean.", textValue));
    }

    /**
     * Return short value
     * @param textValue text value
     * @return short value
     * @throws SQLException in case when the value cannot be converted
     */
    public static short parseShort(String textValue) throws SQLException {
        try {
            return Short.parseShort(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return int value
     * @param textValue text value
     * @return int value
     * @throws SQLException in case when the value cannot be converted
     */
    public static int parseInt(String textValue) throws SQLException {
        try {
            return Integer.parseInt(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return long value
     * @param textValue text value
     * @return long value
     * @throws SQLException in case when the value cannot be converted
     */
    public static long parseLong(String textValue) throws SQLException {
        try {
            return Long.parseLong(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return float value
     * @param textValue text value
     * @return float value
     * @throws SQLException in case when the value cannot be converted
     */
    public static float parseFloat(String textValue) throws SQLException {
        try {
            return Float.parseFloat(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return double value
     * @param textValue text value
     * @return double value
     * @throws SQLException in case when the value cannot be converted
     */
    public static double parseDouble(String textValue) throws SQLException {
        try {
            return Double.parseDouble(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return BigDecimal value
     * @param textValue text value
     * @return BigDecimal value
     * @throws SQLException in case when the value cannot be converted
     */
    public static BigDecimal parseBigDecimal(String textValue) throws SQLException {
        try {
            return new BigDecimal(textValue);
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return BigDecimal value
     * @param textValue text value
     * @param scale precision
     * @return BigDecimal value
     * @throws SQLException in case when the value cannot be converted
     */
    public static BigDecimal parseBigDecimal(String textValue, int scale) throws SQLException {
        try {
            return new BigDecimal(textValue, new MathContext(scale));
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Return Object value
     * @param textValue text value
     * @return object value
     * @throws SQLException in case when the value cannot be converted
     */
    public static Object parseObject(String textValue) throws SQLException {
        try {
                return NumberFormat.getNumberInstance().parse(textValue);
            }
            catch (ParseException e) {
                return textValue;
            }
        }

    /*
    private static List<Class> TYPES = Arrays.asList(
            Short.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class
    );


    public static <T> T parseObject(String textValue, Class<T> type) throws SQLException {
        try {
            for(Class t: TYPES) {
                try {
                    if(t.equals(Short.class)) return parseShort(textValue);

            }
        }
        catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }
 */
}
