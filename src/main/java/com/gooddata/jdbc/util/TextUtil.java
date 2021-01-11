package com.gooddata.jdbc.util;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextUtil {

    public static String extractWorkspaceIdFromUri(String uri) throws SQLException {
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

}
