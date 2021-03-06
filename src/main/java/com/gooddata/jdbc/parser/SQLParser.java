package com.gooddata.jdbc.parser;

import com.gooddata.jdbc.catalog.Catalog;
import com.sun.istack.NotNull;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;

import java.sql.Date;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * SQL parser
 */
public class SQLParser {

    private final static Logger LOGGER = Logger.getLogger(ParsedSQL.class.getName());

    /**
     * Parsed SQL result
     */
    public static class ParsedSQL {

        /**
         * WHERE filter expression
         */
        public static class FilterExpression {

            public static final int OPERATOR_EQUAL = 1;
            public static final int OPERATOR_NOT_EQUAL = 2;
            public static final int OPERATOR_GREATER = 3;
            public static final int OPERATOR_GREATER_OR_EQUAL = 4;
            public static final int OPERATOR_LOWER = 5;
            public static final int OPERATOR_LOWER_OR_EQUAL = 6;
            public static final int OPERATOR_IN = 7;
            public static final int OPERATOR_NOT_IN = 8;
            public static final int OPERATOR_BETWEEN = 9;
            public static final int OPERATOR_NOT_BETWEEN = 10;

            /**
             * Constructor
             * @param operator SQL WHERE operator
             * @param column SQL column
             * @param values WHERE value
             */
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

            private final SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd");

            private String substitutePreparedParameterValue(Object v) throws SQLException {
                if(v instanceof String) {
                    return String.format("'%s'", v);
                } else if (v instanceof Date || v instanceof Time || v instanceof Timestamp) {
                    return dtf.format((Date) v);
                } else if (v instanceof Array) {
                    Object[] s = (Object[]) ((Array) v).getArray();
                    String[] elements = new String[s.length];
                    for(int i = 0; i < s.length; i++) {
                        elements[i] = substitutePreparedParameterValue(s[i]);
                    }
                    return String.join(",", elements);
                }
                else {
                    return String.format("%s", v);
                }
            }

            public int substitutePreparedParameterValues(Map<Integer,Object> preparedStatementParams, int offset)
                    throws SQLException {
                List<String> newValues = new ArrayList<>();
                for (String value : this.values) {
                    if (value.equals("?")) {
                        if (preparedStatementParams.containsKey(offset)) {
                            newValues.add(substitutePreparedParameterValue(preparedStatementParams.get(offset++)));
                        } else {
                            throw new SQLException(String.format("Not enough prepared call parameters. " +
                                    "Can't find parameter at position '%d'", offset));
                        }
                    } else {
                        newValues.add(value);
                    }
                }
                this.values = newValues;
                return offset;
            }


            private int operator;
            private String column;
            private List<String> values;

        }

        public static class OrderByExpression {

            public OrderByExpression(String order, String column) {
                this.order = order;
                this.column = column;
            }

            public String getOrder() {
                return order;
            }

            public String getColumn() {
                return column;
            }

            private final String order;
            private final String column;
        }

        private final List<String> columns;
        private final List<String> tables;
        private final List<FilterExpression> filters;
        private final int limit;
        private final int offset;
        private final List<ParsedSQL.OrderByExpression> orderBys;

        /**
         * Parsed SQL structure - main result from parsing
         * @param columns SQL columns
         * @param tables SQL tables
         * @param filters SQL filters
         * @param orderBys ORDER BY elements
         * @param limit SQL LIMIT
         * @param offset SQL OFFSTE
         */
        public ParsedSQL(List<String> columns, List<String> tables, List<FilterExpression> filters,
                         List<ParsedSQL.OrderByExpression> orderBys,
                         int limit, int offset) {
            this.columns = columns;
            this.tables = tables;
            this.filters = filters;
            this.orderBys = orderBys;
            this.limit = limit;
            this.offset = offset;
        }

        public List<OrderByExpression> getOrderBys() {
            return this.orderBys;
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

        public int getLimit() {
            return limit;
        }

        public int getOffset() {
            return offset;
        }

    }

    private static final JexlEngine jexl = new JexlEngine();

    private static String evaluateExpression(Expression e) {
        if (e instanceof BinaryExpression) {
            org.apache.commons.jexl2.Expression jexlExpr = jexl.createExpression(e.toString());
            return jexlExpr.evaluate(new MapContext()).toString();
        }
        return e.toString();
    }

    /**
     *
     * @param sql parsed SQL
     * @param preparedStatementParams Map of JDBC prepared parameters (starts with idx 1)
     * @return parsed sql with substituted prepared statement's placeholders ('?')
     * @throws SQLException when the substitution fails
     */
    public static SQLParser.ParsedSQL substitutePreparedParams(@NotNull SQLParser.ParsedSQL sql,
                                                               @NotNull Map<Integer, Object> preparedStatementParams) throws SQLException {
        int idx = 1;
        List<SQLParser.ParsedSQL.FilterExpression> filters = sql.getFilters();
        for (SQLParser.ParsedSQL.FilterExpression filter : filters) {
            idx = filter.substitutePreparedParameterValues(preparedStatementParams, idx);
        }
        return sql;
    }

    /**
     * Main parser method for SELECT queries
     * @param query SQL query
     * @return parsed SQL query
     * @throws JSQLParserException wrong syntax
     */
    public static ParsedSQL parseQuery(String query) throws JSQLParserException {
        SQLParser.LOGGER.fine(String.format("Parsing query '%s'", query));
        final List<ParsedSQL> results = new ArrayList<>();
        net.sf.jsqlparser.statement.Statement st = CCJSqlParserUtil.parse(query);
        if (st instanceof Select) {
            Select sl = (Select) st;
            SelectBody sb = sl.getSelectBody();

            final int[] limitAndOffset = new int[]{Integer.MAX_VALUE, 0};
            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            List<ParsedSQL.FilterExpression> filters = new ArrayList<>();

            final List<ParsedSQL.OrderByExpression> orderBys = new ArrayList<>();

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
                            try {
                                // handling SPARK SUBQUERY format
                                // SELECT * FROM (<original select>) SPARK_GEN_SUBQ_0 WHERE 1=0
                                results.add(parseQuery(subSelect.getSelectBody().toString()));
                            } catch (JSQLParserException e) {
                                errors.add(new JSQLParserException(
                                        String.format("Wrong subquery format sql='%s'.",
                                                subSelect.getSelectBody().toString())));
                            }
                            super.visit(subSelect);
                        }

                        public void visit(Table tableName) {
                            SQLParser.LOGGER.fine(String.format("Getting table '%s'", tableName));
                            if (tableName != null)
                                tables.add(tableName.toString().replace("\"", "")
                                );
                            super.visit(tableName);
                        }

                    };
                    if (fromTables != null)
                        fromTables.accept(fv);

                    Limit limitTerm = plainSelect.getLimit();
                    if(limitTerm != null) {
                        limitAndOffset[0] = Integer.parseInt(limitTerm.getRowCount().toString());
                    }
                    Offset offsetTerm = plainSelect.getOffset();
                    if(offsetTerm != null) {
                        limitAndOffset[1] = (int)offsetTerm.getOffset();
                    }

                    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
                    if(orderByElements != null) {
                        for (OrderByElement orderByElement : orderByElements) {
                            orderBys.add(new ParsedSQL.OrderByExpression(
                                    orderByElement.isAsc() ? "ASC" : "DESC"
                                    , orderByElement.getExpression().toString().replace("\"", "")
                            ));
                        }
                    }

                    Expression where = plainSelect.getWhere();
                    ExpressionVisitor ev = new ExpressionVisitorAdapter() {

                        private void handleVisit(BinaryExpression e, int operator) {
                            String columnName = e.getLeftExpression().toString()
                                    .replaceAll("\"", "");
                            String value = evaluateExpression(e.getRightExpression());
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    operator,
                                    columnName,
                                    Collections.singletonList(value));
                            filters.add(f);
                        }

                        public void visit(OrExpression expr) {
                            errors.add(new JSQLParserException("OR logical operators are not supported yet."));
                        }

                        public void visit(NotExpression expr) {
                            errors.add(new JSQLParserException("NOT logical operators are not supported yet."));
                        }

                        public void visit(EqualsTo expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_EQUAL);
                            super.visit(expr);
                        }

                        public void visit(GreaterThan expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_GREATER);
                            super.visit(expr);
                        }

                        public void visit(GreaterThanEquals expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_GREATER_OR_EQUAL);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(MinorThan expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_LOWER);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(MinorThanEquals expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_LOWER_OR_EQUAL);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(NotEqualsTo expr) {
                            handleVisit(expr, ParsedSQL.FilterExpression.OPERATOR_NOT_EQUAL);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(InExpression expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            ExpressionList expressionValues = expr.getRightItemsList(ExpressionList.class);
                            List<String> values = expressionValues.getExpressions().stream()
                                    .map(SQLParser::evaluateExpression)
                                    .collect(Collectors.toList());
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    expr.isNot() ? ParsedSQL.FilterExpression.OPERATOR_NOT_IN:
                                            ParsedSQL.FilterExpression.OPERATOR_IN,
                                    columnName,
                                    values);
                            filters.add(f);
                            super.visit(expr);
                        }

                        @Override
                        public void visit(Between expr) {
                            String columnName = expr.getLeftExpression().toString()
                                    .replaceAll("\"","");
                            ParsedSQL.FilterExpression f = new ParsedSQL.FilterExpression(
                                    expr.isNot() ? ParsedSQL.FilterExpression.OPERATOR_NOT_BETWEEN:
                                            ParsedSQL.FilterExpression.OPERATOR_BETWEEN,
                                    columnName,
                                    Arrays.asList(
                                            evaluateExpression(expr.getBetweenExpressionStart()),
                                            evaluateExpression(expr.getBetweenExpressionEnd())));
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
            if ( results.size()>0 ) {
                return results.get(0);
            }
            else {
                return new ParsedSQL(columns, tables, filters, orderBys, limitAndOffset[0], limitAndOffset[1]);
            }
        } else {
            throw new JSQLParserException("Only SELECT SQL statements are supported.");
        }
    }

    /**
     * Conversion between datatype name to SQL type int
     * @param sqlTypeName datatype name
     * @return java.sql datatype representation
     */
    public static int convertSQLDataTypeNameToJavaSQLType(String sqlTypeName)  {
        if(sqlTypeName.equalsIgnoreCase("VARCHAR"))
            return Types.VARCHAR;
        if(sqlTypeName.equalsIgnoreCase("NUMERIC"))
            return Types.NUMERIC;
        if(sqlTypeName.equalsIgnoreCase("DECIMAL"))
            return Types.DECIMAL;
        if(sqlTypeName.equalsIgnoreCase("DOUBLE"))
            return Types.DOUBLE;
        if(sqlTypeName.equalsIgnoreCase("FLOAT"))
            return Types.FLOAT;
        if(sqlTypeName.equalsIgnoreCase("INTEGER"))
            return Types.INTEGER;
        if(sqlTypeName.equalsIgnoreCase("CHAR"))
            return Types.CHAR;
        if(sqlTypeName.equalsIgnoreCase("DATE"))
            return Types.DATE;
        if(sqlTypeName.equalsIgnoreCase("TIME"))
            return Types.TIME;
        if(sqlTypeName.equalsIgnoreCase("DATETIME") || sqlTypeName.equalsIgnoreCase("TIMESTAMP"))
            return Types.TIMESTAMP;
        throw new RuntimeException(String.format("Data type '%s' is not supported.", sqlTypeName));
    }

    /**
     * Conversion between datatype name to Java datatype classname
     * @param sqlTypeName datatype name
     * @return Java datatype classname
     */
    public static String convertSQLDataTypeNameToJavaClassName(String sqlTypeName)  {
        if(sqlTypeName.equalsIgnoreCase("VARCHAR"))
            return "java.lang.String";
        if(sqlTypeName.equalsIgnoreCase("NUMERIC"))
            return "java.math.BigDecimal";
        if(sqlTypeName.equalsIgnoreCase("DECIMAL"))
            return "java.math.BigDecimal";
        if(sqlTypeName.equalsIgnoreCase("DOUBLE"))
            return "java.lang.Double";
        if(sqlTypeName.equalsIgnoreCase("FLOAT"))
            return "java.lang.Float";
        if(sqlTypeName.equalsIgnoreCase("INTEGER"))
            return "java.lang.Integer";
        if(sqlTypeName.equalsIgnoreCase("CHAR"))
            return "java.lang.String";
        if(sqlTypeName.equalsIgnoreCase("DATE"))
            return "java.sql.Date";
        if(sqlTypeName.equalsIgnoreCase("TIME"))
            return "java.sql.Time";
        if(sqlTypeName.equalsIgnoreCase("DATETIME") || sqlTypeName.equalsIgnoreCase("TIMESTAMP"))
            return "java.sql.Timestamp";
        throw new RuntimeException(String.format("Data type '%s' is not supported.", sqlTypeName));
    }

    /**
     * Parsed SQL datatype e.g. VARCHAR(255) or DECIMAL(13,2)
     */
    public static class ParsedSQLDataType {

        /**
         * Constructor
         * @param name datatype name
         * @param size datatype size
         * @param precision datatype precision
         */
        public ParsedSQLDataType(String name, int size, int precision) {
            this.name = name;
            this.size = size;
            this.precision = precision;
        }

        public ParsedSQLDataType(String name) {
            this.name = name;
        }

        public ParsedSQLDataType(String name, int size) {
            this.name = name;
            this.size = size;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public int getPrecision() {
            return precision;
        }

        public void setPrecision(int precision) {
            this.precision = precision;
        }

        private String name;
        private int size;
        private int precision;
    }

    public static class ParsedColumnName {

        private final String name;
        private final String datatype;

        public ParsedColumnName(String name, String datatype) {
            this.name = name;
            this.datatype = datatype;
        }

        public String getName() {
            return name;
        }

        public String getDatatype() {
            return datatype;
        }
    }

    /**
     * Parses column's datatype extension format (e.g. "REVENUE::INTEGER", "REVENUE::VARCHAR(255)")
     * @param columnName column name with the datatype specifier
     * @return parsed structure with name and datatype
     */
    public static ParsedColumnName parseColumnWithDataTypeSpecifier(String columnName)
            throws Catalog.CatalogEntryNotFoundException {
        if (columnName.contains("::")) {
            String[] parsedColumn = Arrays.stream(columnName.split("::"))
                    .map(String::trim).toArray(String[]::new);
            if (parsedColumn.length != 2)
                throw new Catalog.CatalogEntryNotFoundException(String.format(
                        "Invalid column name format '%s'.", columnName));
            return new ParsedColumnName(parsedColumn[0], parsedColumn[1]);
        }
        else {
            return new ParsedColumnName(columnName, null);
        }
    }


    /**
     * Parses SQL datatype e.g. VARCHAR(255) or DECIMAL(13,2)
     * @param dataType datatype text
     * @return parsed datatype
     */
    public static ParsedSQLDataType parseSqlDatatype(String dataType) {
        String dataTypeName;
        int size = 0;
        int precision = 0;
        Pattern p1 = Pattern.compile("^\\s?([a-zA-Z]+)\\s?(\\(\\s?([0-9]+)\\s?(\\s?,\\s?([0-9]+)\\s?)?\\s?\\))?\\s?$");
        Matcher m1 = p1.matcher(dataType);
        boolean b = m1.matches();
        int cnt = m1.groupCount();
        dataTypeName = m1.group(1);
        String sizeTxt = m1.group(3);
        if(sizeTxt!=null)
            size = Integer.parseInt(sizeTxt);
        String precisionTxt = m1.group(5);
        if(precisionTxt!=null)
            precision = Integer.parseInt(precisionTxt);
        return new ParsedSQLDataType(dataTypeName, size, precision);
    }


}
