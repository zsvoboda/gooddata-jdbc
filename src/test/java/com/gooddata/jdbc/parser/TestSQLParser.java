package com.gooddata.jdbc.parser;

import net.sf.jsqlparser.JSQLParserException;
import org.testng.annotations.Test;

public class TestSQLParser {

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

        parsedSQL = parser.parseQuery("SELECT c1,c2 FROM t1 WHERE c1 IN ('v1','v2','v3')");
        assert(parsedSQL.getFilters().get(0).getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_IN);
        assert(parsedSQL.getFilters().get(0).getValues().contains("v1"));
        assert(parsedSQL.getFilters().get(0).getValues().contains("v2"));
        assert(parsedSQL.getFilters().get(0).getValues().contains("v3"));

        parsedSQL = parser.parseQuery("SELECT \"Product Category\",\"# of Orders\" " +
                "WHERE \"Product Category\" NOT IN ('Outdoor', 'Clothing')");
        assert(parsedSQL.getFilters().get(0).getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_IN);
        assert(parsedSQL.getFilters().get(0).getValues().contains("Outdoor"));
        assert(parsedSQL.getFilters().get(0).getValues().contains("Clothing"));

        parsedSQL = parser.parseQuery("SELECT \"Product Category\",\"# of Orders\" " +
                "WHERE \"Product Category\" NOT IN ('Outdoor', 'Clothing') AND \"Revenue\" NOT BETWEEN 1000 AND 10000");
        assert(parsedSQL.getFilters().get(1).getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_NOT_BETWEEN);
        assert(parsedSQL.getFilters().get(1).getValues().get(0).equals("1000"));
        assert(parsedSQL.getFilters().get(1).getValues().get(1).equals("10000"));

        parsedSQL = parser.parseQuery("SELECT \"[/gdc/a]\", \"[/gdc/b]\" WHERE \"[/gdc/c]\" BETWEEN 1 AND 2");
        assert(parsedSQL.getColumns().contains("[/gdc/a]"));
        assert(parsedSQL.getColumns().contains("[/gdc/b]"));
        assert(parsedSQL.getFilters().get(0).getColumn().equals("[/gdc/c]"));
        assert(parsedSQL.getFilters().get(0).getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_BETWEEN);
        assert(parsedSQL.getFilters().get(0).getValues().contains("1"));
        assert(parsedSQL.getFilters().get(0).getValues().contains("2"));

        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE m1 = (3*5+3)/6");
        assert(parsedSQL.getColumns().contains("c1"));
        assert(parsedSQL.getColumns().contains("c2"));
        assert(parsedSQL.getColumns().contains("m1"));
        assert(parsedSQL.getTables().contains("t1"));
        assert(parsedSQL.getFilters().get(0).getColumn().equals("m1"));
        assert(parsedSQL.getFilters().get(0).getOperator() == SQLParser.ParsedSQL.FilterExpression.OPERATOR_EQUAL);
        assert(parsedSQL.getFilters().get(0).getValues().get(0).equals("3"));

        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE m1 BETWEEN (3*5+3)/6 AND (6*5+6)/6");
        assert(parsedSQL.getFilters().get(0).getValues().get(0).equals("3"));
        assert(parsedSQL.getFilters().get(0).getValues().get(1).equals("6"));

        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1 = 'Home'");
        assert(parsedSQL.getFilters().get(0).getValues().get(0).equals("Home"));

        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1= 'Home'");
        assert(parsedSQL.getFilters().get(0).getValues().get(0).equals("Home"));

        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1='Home '");
        assert(parsedSQL.getFilters().get(0).getValues().get(0).equals("Home "));

    }

    @Test
    public void testLimit() throws JSQLParserException {
        SQLParser parser = new SQLParser();
        SQLParser.ParsedSQL parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1='Home' LIMIT 10 OFFSET 3");
        assert(parsedSQL.getOffset() == 3);
        assert(parsedSQL.getLimit() == 10);
    }

    @Test
    public void testOrderBy() throws JSQLParserException {
        SQLParser parser = new SQLParser();
        SQLParser.ParsedSQL parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1='Home' ORDER BY 1, 2 ASC, 3 DESC");
        assert(parsedSQL.getOrderBys().get(0).getOrder().equals("ASC"));
        assert(parsedSQL.getOrderBys().get(0).getColumn().equals("1"));
        assert(parsedSQL.getOrderBys().get(1).getOrder().equals("ASC"));
        assert(parsedSQL.getOrderBys().get(1).getColumn().equals("2"));
        assert(parsedSQL.getOrderBys().get(2).getOrder().equals("DESC"));
        assert(parsedSQL.getOrderBys().get(2).getColumn().equals("3"));
        parsedSQL = parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1='Home' ORDER BY \"c1\" ASC, c2 DESC, m1");
        assert(parsedSQL.getOrderBys().get(0).getOrder().equals("ASC"));
        assert(parsedSQL.getOrderBys().get(0).getColumn().equals("c1"));
        assert(parsedSQL.getOrderBys().get(1).getOrder().equals("DESC"));
        assert(parsedSQL.getOrderBys().get(1).getColumn().equals("c2"));
        assert(parsedSQL.getOrderBys().get(2).getOrder().equals("ASC"));
        assert(parsedSQL.getOrderBys().get(2).getColumn().equals("m1"));

    }

    @Test(expectedExceptions = { JSQLParserException.class })
    public void testParseException() throws JSQLParserException {
        SQLParser parser = new SQLParser();
        parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1='Home ");
        parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1 IN () ");
        parser.parseQuery("SELECT c1,c2,m1 FROM t1 WHERE c1 BETWEEN 2 ");
    }


}
