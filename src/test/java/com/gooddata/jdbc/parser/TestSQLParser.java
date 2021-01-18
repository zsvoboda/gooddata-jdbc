package com.gooddata.jdbc.parser;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

import java.util.logging.Logger;

public class TestSQLParser {

    private final static Logger LOGGER = Logger.getLogger(TestSQLParser.class.getName());

    @Test
    public void testParse() throws JSQLParserException {
        SQLParser parser = new SQLParser();
        SQLParser.ParsedSQL parsedSQL = parser.parseQuery("SELECT c1,c2 FROM t1");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));
        parsedSQL = parser.parseQuery("SELECT \"c1\",\"c2\" FROM \"t1\"");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));

        parser.parseQuery("SELECT c1,c2 FROM t1 WHERE c1 IN ('v1','v2','v3')");
        parser.parseQuery("SELECT \"Product Category\",\"# of Orders\" " +
                "WHERE \"Product Category\" NOT IN ('Outdoor', 'Clothing')");
        parser.parseQuery("SELECT \"Product Category\",\"# of Orders\" " +
                "WHERE \"Product Category\" NOT IN ('Outdoor', 'Clothing') AND \"Revenue\" NOT BETWEEN 1000 AND 10000");

    }

}
