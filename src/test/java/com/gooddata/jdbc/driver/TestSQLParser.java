package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

public class TestSQLParser {


    private SQLParser parser = new SQLParser();

    @Test
    public void testParse() throws JSQLParserException {
        SQLParser.ParsedSQL parsedSQL = parser.parse("SELECT c1,c2 FROM t1");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));
        parsedSQL = parser.parse("SELECT \"c1\",\"c2\" FROM \"t1\"");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));
    }

}
