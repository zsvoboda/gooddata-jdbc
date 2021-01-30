package com.gooddata.jdbc.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Useful text utils
 */
public class TextUtil {

    /**
     * Extracts ID from URI
     * @param uri URI
     * @return ID
     * @throws InvalidFormatException syntax issue
     */
    public static String extractIdFromUri(String uri) throws InvalidFormatException {
        try {
            Pattern p = Pattern.compile("^\\s?/gdc/md/(.*?)/obj/(.*?)\\s?$");
            Matcher m = p.matcher(uri);
            boolean b = m.matches();
            if (!b || m.groupCount() != 2)
                throw new InvalidFormatException(String.format("Wrong URI format: '%s'", uri));
            return m.group(1);
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong URI format: '%s'", uri));
        }
    }

    /**
     * Finds out whether passed string is GoodData object URI or not
     * @param uri URI
     * @return true or false
     * @throws InvalidFormatException syntax issue
     */
    public static boolean isGoodDataObjectUri(String uri) throws InvalidFormatException {
        try {
            Pattern p = Pattern.compile("^\\s?/gdc/md/(.*?)/obj/(.*?)\\s?$");
            Matcher m = p.matcher(uri);
            return m.matches();
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong URI format: '%s'", uri));
        }
    }

    /**
     * Finds out whether passed string is a column with GoodData URI or not
     * Test format [/gdc/md/<project-id>/obj/<id>]
     * @param columnName columnName
     * @return true or false
     * @throws InvalidFormatException syntax issue
     */
    public static boolean isGoodDataColumnWithUri(String columnName) throws InvalidFormatException {
        try {
            Pattern p = Pattern.compile("^\\s?\\[\\s?/gdc/md/(.*?)/obj/(.*?)\\s?]\\s?$");
            Matcher m = p.matcher(columnName);
            return m.matches();
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong column format: '%s'", columnName));
        }
    }

    /**
     * Extracts object URI from the column spec format [/gdc/md/<project-id>/obj/<id>]
     * @param columnName columnName
     * @return URI
     * @throws InvalidFormatException syntax issue
     */
    public static String extractGoodDataUriFromColumnName(String columnName) throws InvalidFormatException {
        try {
            Pattern p = Pattern.compile("^\\s?\\[\\s?(/gdc/md/(.*?)/obj/(.*?))\\s?]\\s?$");
            Matcher m = p.matcher(columnName);
            boolean b = m.matches();
            int groupCnt = m.groupCount();
            if (!b || groupCnt != 3)
                throw new InvalidFormatException(String.format("Wrong column format: '%s'", columnName));
            return m.group(1);
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong column format: '%s'", columnName));
        }
    }

    /**
     * Finds all object uris in MAQL expression
     * @param maqlExpression MAQL
     * @return URI
     * @throws InvalidFormatException syntax issue
     */
    public static List<String> findAllObjectUris(String maqlExpression) throws InvalidFormatException {
        try {
            List<String> uris = new ArrayList<>();
            Pattern p = Pattern.compile("\\[(/gdc/md/[a-z0-9]+/obj/[0-9]+)]");
            Matcher m = p.matcher(maqlExpression);
            while (m.find()) {
                for (int i = 1; i <= m.groupCount(); i++) {
                    uris.add(m.group(i));
                }
            }
            return uris;
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong MAQL format: '%s'", maqlExpression));
        }
    }

    /**
     * Finds all element uris in MAQL expression
     * @param maqlExpression MAQL
     * @return URI
     * @throws InvalidFormatException syntax issue
     */
    public static List<String> findAllElementUris(String maqlExpression) throws InvalidFormatException {
        try {
            List<String> uris = new ArrayList<>();
            Pattern p = Pattern.compile("\\[(/gdc/md/[a-z0-9]+/obj/[0-9]+/elements\\?id=[0-9]+)]");
            Matcher m = p.matcher(maqlExpression);
            while (m.find()) {
                for (int i = 1; i <= m.groupCount(); i++) {
                    uris.add(m.group(i));
                }
            }
            return uris;
        } catch (IllegalStateException e) {
            throw new InvalidFormatException(String.format("Wrong MAQL format: '%s'", maqlExpression));
        }
    }


    /**
     * Contains case insensitive
     * @param l list of strins
     * @param s compared string
     * @return true if the list contains the string
     */
    public static boolean containsIgnoreCase(List<String> l, String s) {
        return l.stream().anyMatch(s::equalsIgnoreCase);
    }

    /**
     * This exception is thrown when there is an invalid textual format of a column
     */
    public static class InvalidFormatException extends Exception {
        public InvalidFormatException(String e) {
            super(e);
        }
    }

}
