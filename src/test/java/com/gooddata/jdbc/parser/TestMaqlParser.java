package com.gooddata.jdbc.parser;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

public class TestMaqlParser {

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

        metric = maqlParser.parseCreateOrAlterMetric(
                "CREATE METRIC \"Yearly Revenue\" AS SELECT SUM(\"Revenue\") BY \"Year (Date)\" ALL OTHERS");
        assert(metric.getName().equals("Yearly Revenue"));
        assert(metric.getLdmObjectTitles().contains("Revenue"));
        assert(metric.getLdmObjectTitles().contains("Year (Date)"));

        metric = maqlParser.parseCreateOrAlterMetric(
                "CREATE METRIC \"Yearly Revenue\" AS SELECT SUM(\"Revenue\") BY \"Year (Date)\" ALL OTHERS " +
                        "WHERE \"Product Category\" IN ('Home', 'Electronics')");
        assert(metric.getName().equals("Yearly Revenue"));
        assert(metric.getLdmObjectTitles().contains("Revenue"));
        assert(metric.getLdmObjectTitles().contains("Year (Date)"));
        assert(metric.getLdmObjectTitles().contains("Product Category"));
        assert(metric.getAttributeElementValues().contains("Home"));
        assert(metric.getAttributeElementValues().contains("Electronics"));
        assert(metric.getAttributeElementToAttributeNameLookup().get("Electronics").equals("Product Category"));

    }

    @Test
    public void testParseDropMetric() throws JSQLParserException {
        MaqlParser maqlParser = new MaqlParser();
        String metric = maqlParser.parseDropOrDescribeMetric(
                "DROP METRIC \"test\"");
        assert("test".equals(metric));
    }

    @Test(expectedExceptions = { JSQLParserException.class })
    public void testUnquotedIdentifier() throws JSQLParserException {
        MaqlParser maqlParser = new MaqlParser();
        maqlParser.parseDropOrDescribeMetric(
                "CREATE METRIC \"ORDER_AMOUNT_METRIC\" AS SELECT SUM(ORDER_QUANTITY * PRODUCT_PRICE)");
        maqlParser.parseDropOrDescribeMetric(
                "ALTER METRIC \"ORDER_AMOUNT_METRIC\" AS SELECT SUM(\"ORDER_QUANTITY\" * PRODUCT_PRICE)");
        maqlParser.parseDropOrDescribeMetric(
                "DROP METRIC ORDER_AMOUNT_METRIC");

    }

}
