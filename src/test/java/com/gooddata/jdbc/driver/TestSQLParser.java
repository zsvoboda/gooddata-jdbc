package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

import java.util.logging.Logger;

public class TestSQLParser {

    private final static Logger LOGGER = Logger.getLogger(TestSQLParser.class.getName());

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
