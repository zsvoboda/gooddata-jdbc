package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

import java.util.logging.Logger;

public class TestSQLParser {

    private final static Logger LOGGER = Logger.getLogger(TestSQLParser.class.getName());

    private final SQLParser parser = new SQLParser();

    @Test
    public void testParse() throws JSQLParserException {
        SQLParser.ParsedSQL parsedSQL = parser.parseQuery("SELECT c1,c2 FROM t1");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));
        parsedSQL = parser.parseQuery("SELECT \"c1\",\"c2\" FROM \"t1\"");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getTables().contains("t1"));

        parsedSQL = parser.parseQuery("SELECT c1,c2 FROM t1 WHERE c1 IN ('v1','v2','v3')");
        parsedSQL = parser.parseQuery("SELECT \"Product Category\",\"# of Orders\" " +
                "WHERE \"Product Category\" NOT IN ('Outdoor', 'Clothing')");

    }

    @Test
    public void testParseCreateMetric() throws JSQLParserException {
        SQLParser.ParsedCreateMetricStatement metric = parser.parseCreateOrAlterMetric(
                "CREATE METRIC \"test\" AS SELECT SUM(\"Revenue\") BY \"Product Category\" " +
                        "WHERE \"Product Category\" IN ('Home', 'Electronics')");
        assert("test".equals(metric.getName()));
        assert("SELECT SUM(\"Revenue\") BY \"Product Category\" WHERE \"Product Category\" IN ('Home', 'Electronics')".equals(metric.getMetricMaqlDefinition()));
        assert(metric.getLdmObjectTitles().size() == 3);
        assert(metric.getLdmObjectTitles().contains("Product Category"));
        assert(metric.getLdmObjectTitles().contains("Revenue"));

        metric = parser.parseCreateOrAlterMetric(
                "ALTER METRIC \"test\" AS SELECT SUM(\"Revenue\") BY \"Product Category\" " +
                        "WHERE \"Product Category\" IN ('Home', 'Electronics')");
        assert("test".equals(metric.getName()));
        assert("SELECT SUM(\"Revenue\") BY \"Product Category\" WHERE \"Product Category\" IN ('Home', 'Electronics')".equals(metric.getMetricMaqlDefinition()));
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
