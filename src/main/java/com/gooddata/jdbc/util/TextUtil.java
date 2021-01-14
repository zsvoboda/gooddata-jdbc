package com.gooddata.jdbc.util;

import java.sql.SQLException;
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
     * @throws SQLException syntax issue
     */
    public static String extractIdFromUri(String uri) throws SQLException {
        try {
            Pattern p = Pattern.compile("^/gdc/md/(.*?)/obj/(.*?)$");
            Matcher m = p.matcher(uri);
            m.matches();
            if (m.groupCount() != 2)
                throw new SQLException(String.format("Wrong JDBC URL format: '%s'", uri));
            return m.group(1);
        } catch (IllegalStateException e) {
            throw new SQLException(String.format("Wrong URI format: '%s'", uri));
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

}
