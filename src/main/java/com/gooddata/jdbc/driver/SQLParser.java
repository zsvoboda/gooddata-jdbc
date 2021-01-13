package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLParser {

    public static class ParsedSQL {

        public static class FilterExpression {

            public static final int OPERATOR_EQUAL = 1;
            public static final int OPERATOR_NOT_EQUAL = 2;
            public static final int OPERATOR_GREATER = 3;
            public static final int OPERATOR_GREATER_OR_EQUAL = 4;
            public static final int OPERATOR_LOWER = 5;
            public static final int OPERATOR_LOWER_OR_EQUAL = 6;

            public FilterExpression(int operator, String column, List<String> values) {
                this.operator = operator;
                this.column = column;
                this.values = values;
            }

            public int getOperator() {
                return operator;
            }

            public void setOperator(int operator) {
                this.operator = operator;
            }

            public String getColumn() {
                return column;
            }

            public void setColumn(String column) {
                this.column = column;
            }

            public List<String> getValues() {
                return values;
            }

            public void setValues(List<String> values) {
                this.values = values;
            }

            private int operator;
            private String column;
            private List<String> values;

        }

        private final static Logger LOGGER = Logger.getLogger(ParsedSQL.class.getName());

        private final List<String> columns;
        private final List<String> tables;
        private final List<FilterExpression> filters;

        public ParsedSQL(List<String> columns, List<String> tables, List<FilterExpression> filters) {
            this.columns = columns;
            this.tables = tables;
            this.filters = filters;
        }

        public List<String> getColumns() {
            return this.columns;
        }

        public List<String> getTables() {
            return this.tables;
        }

        public List<FilterExpression> getFilters() {
            return this.filters;
        }

    }


    public static Expression evaluateExpression(Expression e) {

        List<Expression> r = new ArrayList<>();

        ExpressionVisitor ev = new ExpressionVisitorAdapter() {

            @Override
            public void visit(LongValue longValue) {
                super.visit(longValue);
                r.add(longValue);
            }

        };
        if (e != null)
            e.accept(ev);
        return r.get(0);

    }

    public ParsedSQL parse(String query) throws JSQLParserException {
        ParsedSQL.LOGGER.fine(String.format("Parsing query '%s'", query));
        net.sf.jsqlparser.statement.Statement st = CCJSqlParserUtil.parse(query);
        if (st instanceof Select) {
            Select sl = (Select) st;
            SelectBody sb = sl.getSelectBody();

            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            List<ParsedSQL.FilterExpression> filters = new ArrayList<>();

            final List<JSQLParserException> errors = new ArrayList<>();
            SelectVisitor sv = new SelectVisitorAdapter() {

                public void visit(PlainSelect plainSelect) {
                    plainSelect.getSelectItems().forEach((item) -> {
                        // TODO implement expressions and functions
                        columns.add(item.toString().replace("\"", ""));
                    });
                    FromItem fromTables = plainSelect.getFromItem();
                    FromItemVisitor fv = new FromItemVisitorAdapter() {

                        public void visit(SubJoin subjoin) {
                            errors.add(new JSQLParserException("JOIN queries aren't supported."));
                            super.visit(subjoin);
                        }

                        public void visit(SubSelect subSelect) {
                            errors.add(new JSQLParserException("Subqueries queries aren't supported."));
                            super.visit(subSelect);
                        }

                        public void visit(Table tableName) {
                            ParsedSQL.LOGGER.fine(String.format("Getting table '%s'", tableName));
                            if (tableName != null)
                                tables.add(tableName.toString().replace("\"", "")
                                );
                            super.visit(tableName);
                        }

                    };
                    if (fromTables != null)
                        fromTables.accept(fv);

                    // TODO WHERE expressions

                    Expression where = plainSelect.getWhere();
                    ExpressionVisitor ev = new ExpressionVisitorAdapter() {

                        public void visit(EqualsTo expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_EQUAL,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                        public void visit(GreaterThan expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_GREATER,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                        public void visit(OrExpression expr) {
                            errors.add(new JSQLParserException("OR logical operators are not supported yet."));
                        }

                        public void visit(NotExpression expr) {
                            errors.add(new JSQLParserException("NOT logical operators are not supported yet."));
                        }

                        public void visit(GreaterThanEquals expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_GREATER_OR_EQUAL,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(MinorThan expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_LOWER,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(MinorThanEquals expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_LOWER_OR_EQUAL,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(NotEqualsTo expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            String value = expr.getRightExpression().toString()
                                    .replaceAll("'","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL,
                                    columnName,
                                    Arrays.asList(value));
                            filters.add(f);
                            super.visit(expr);
                        }

                    };
                    if (where != null)
                        where.accept(ev);
                    super.visit(plainSelect);
                }
            };
            sb.accept(sv);
            if (errors.size() > 0) {
                throw errors.get(0);
            }
            return new ParsedSQL(columns, tables, filters);
        } else {
            throw new JSQLParserException("Only SELECT SQL statements are supported.");
        }
    }

    public static class ParsedCreateMetricStatement {

        public ParsedCreateMetricStatement(String name, String metricMaqlDefinition, Set<String> ldmObjectTitles,
                                           Set<String> attributElementValues) {
            this.metricMaqlDefinition = metricMaqlDefinition;
            this.ldmObjectTitles = ldmObjectTitles;
            this.attributElementValues = attributElementValues;
            this.name = name;
        }

        public String getMetricMaqlDefinition() {
            return metricMaqlDefinition;
        }

        public void setMetricMaqlDefinition(String metricMaqlDefinition) {
            this.metricMaqlDefinition = metricMaqlDefinition;
        }

        public Set<String> getLdmObjectTitles() {
            return ldmObjectTitles;
        }

        public void setLdmObjectTitles(Set<String> ldmObjectTitles) {
            this.ldmObjectTitles = ldmObjectTitles;
        }

        public Set<String> getAttributElementValues() {
            return attributElementValues;
        }

        public void setAttributElementValues(Set<String> attributElementValues) {
            this.attributElementValues = attributElementValues;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        private String metricMaqlDefinition;
        private Set<String> ldmObjectTitles;
        private Set<String> attributElementValues;
        private String name;
    }

    public ParsedCreateMetricStatement parseCreateMetric(String sql) throws JSQLParserException {
        String sqlWithNoNewlines = sql.replaceAll("\n"," ");
        Pattern p = Pattern.compile(
                "^\\s?create\\s+metric\\s+\"(.*?)\"\\s+as\\s+(.*?)\\s?[;]?\\s?$",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        m.matches();
        if (m.groupCount() != 2)
            throw new JSQLParserException(String.format("Wrong CREATE METRIC syntax: '%s'", sql));
        String metricName = m.group(1);
        String metricMaql = m.group(2);

        Set<String> factsMetricsOrAttributeTitles = new HashSet<>();
        String s = metricMaql;
        Pattern p1 = Pattern.compile("(\"[a-zA-Z ]+\")");
        Matcher m1 = p1.matcher(s);
        while (m1.find()) {
            factsMetricsOrAttributeTitles.add(m1.group(1).replaceAll("\"",""));
            s = s.substring(m1.start() + 1);
            m1 = p1.matcher(s);
        }

        Set<String> attributeElementValues = new HashSet<>();
        s = metricMaql;
        Pattern p2 = Pattern.compile("(\'[a-zA-Z ]+\')");
        Matcher m2 = p2.matcher(s);
        while (m2.find()) {
            attributeElementValues.add(m2.group(1).replaceAll("'",""));
            s = s.substring(m2.start() + 1);
            m2 = p2.matcher(s);
        }

        ParsedCreateMetricStatement metric = new ParsedCreateMetricStatement(metricName, metricMaql,
                factsMetricsOrAttributeTitles, attributeElementValues);

        return metric;
    }

    public String parseDropMetric(String sql) throws JSQLParserException {
        String sqlWithNoNewlines = sql.replaceAll("\n"," ");
        Pattern p = Pattern.compile(
                "^\\s?drop\\s+metric\\s+\"(.*?)\"\\s?[;]?\\s?$",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sql);
        m.matches();
        if (m.groupCount() != 1)
            throw new JSQLParserException(String.format("Wrong DROP METRIC syntax: '%s'", sql));
        String metricName = m.group(1);

        return metricName;
    }

}
