package com.gooddata.jdbc.util;

import com.gooddata.jdbc.driver.Driver;
import com.sun.istack.NotNull;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class DataTypeParser {

    private final static Logger LOGGER = Logger.getLogger(DataTypeParser.class.getName());

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
    public static boolean parseBoolean(@NotNull String textValue) throws SQLException {
        if(textValue == null) return false;
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
    public static short parseShort(@NotNull String textValue) throws SQLException {
        if(textValue == null) return 0;
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
    public static int parseInt(@NotNull String textValue) throws SQLException {
        if(textValue == null) return 0;
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
    public static long parseLong(@NotNull String textValue) throws SQLException {
        if(textValue == null) return 0;
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
    public static float parseFloat(@NotNull String textValue) throws SQLException {
        if(textValue == null) return 0;
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
    public static double parseDouble(@NotNull String textValue) throws SQLException {
        if(textValue == null) return 0;
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
    public static BigDecimal parseBigDecimal(@NotNull String textValue) throws SQLException {
        if(textValue == null) return null;
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
    public static BigDecimal parseBigDecimal(@NotNull String textValue, int scale) throws SQLException {
        if(textValue == null) return null;
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
    public static Object parseObject(@NotNull String textValue) throws SQLException {
        if(textValue == null) return null;
        try {
                return parseInt(textValue);
            }
            catch (SQLException e) {
                try {
                    return parseLong(textValue);
                } catch(SQLException e1) {
                    try {
                        return parseDouble(textValue);
                    } catch(SQLException e2) {
                        return textValue;
                    }
                }
            }
        }

}
