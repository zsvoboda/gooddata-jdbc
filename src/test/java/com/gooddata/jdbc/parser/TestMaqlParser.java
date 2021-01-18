package com.gooddata.jdbc.parser;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

import java.util.logging.Logger;

public class TestMaqlParser {

    private final static Logger LOGGER = Logger.getLogger(TestMaqlParser.class.getName());

    @Test
    public void testParseCreateMetric() throws JSQLParserException {
        MaqlParser maqlParser = new MaqlParser();
        MaqlParser.ParsedCreateMetricStatement metric = maqlParser.parseCreateOrAlterMetric(
                "CREATE METRIC \"test\" AS SELECT SUM(\"Revenue\") BY \"Product Category\" " +
                        "WHERE \"Product Category\" IN ('Home', 'Electronics')");
        assert("test".equals(metric.getName()));
        assert("SELECT SUM(\"Revenue\") BY \"Product Category\" WHERE \"Product Category\" IN ('Home', 'Electronics')".equals(metric.getMetricMaqlDefinition()));
        assert(metric.getLdmObjectTitles().size() == 3);
        assert(metric.getLdmObjectTitles().contains("Product Category"));
        assert(metric.getLdmObjectTitles().contains("Revenue"));

        metric = maqlParser.parseCreateOrAlterMetric(
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
        MaqlParser maqlParser = new MaqlParser();
        String metric = maqlParser.parseDropMetric(
                "DROP METRIC \"test\"");
        assert("test".equals(metric));
    }


}
