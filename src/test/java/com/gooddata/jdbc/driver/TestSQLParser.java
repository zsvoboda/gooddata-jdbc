package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

import java.util.logging.Logger;

public class TestSQLParser {

    private final static Logger LOGGER = Logger.getLogger(TestSQLParser.class.getName());

    private final SQLParser parser = new SQLParser();

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

    @Test
    public void testParseExpression() throws JSQLParserException {
        SQLParser.ParsedSQL parsedSQL = parser.parse("SELECT REVENUE WHERE REVENUE > (3+4)*(4+5)");
    }

    @Test
    public void testParseCreateMetric() throws JSQLParserException {
        SQLParser.ParsedCreateMetricStatement metric = parser.parseCreateMetric(
                "CREATE METRIC \"test\" AS SELECT SUM(\"Revenue\") BY \"Product Category\" " +
                        "WHERE \"Product Category\" IN ('Home', 'Appliances')");
        assert("test".equals(metric.getName()));
        assert("SELECT SUM(\"Revenue\") BY \"Product Category\" WHERE \"Product Category\" IN ('Home', 'Appliances')".equals(metric.getMetricMaqlDefinition()));
        assert(metric.getLdmObjectTitles().size() == 3);
        assert(metric.getLdmObjectTitles().contains("Product Category"));
        assert(metric.getLdmObjectTitles().contains("Revenue"));
    }

    @Test
    public void testParseDropMetric() throws JSQLParserException {
        String metric = parser.parseDropMetric(
                "DROP METRIC \"test\"");
        assert("test".equals(metric));
    }


}
