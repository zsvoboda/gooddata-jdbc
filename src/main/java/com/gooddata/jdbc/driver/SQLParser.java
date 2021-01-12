package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.tools.ant.types.resources.Union;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class SQLParser {

    public static class ParsedSQL {

        private final static Logger LOGGER = Logger.getLogger(ParsedSQL.class.getName());

        private final List<String> columns;
        private final List<String> tables;
        private final List<String> filters;

        public ParsedSQL(List<String> columns, List<String> tables, List<String> filters) {
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

        public List<String> getFilters() {
            return this.filters;
        }

    }

    public ParsedSQL parse(String query) throws JSQLParserException {
        ParsedSQL.LOGGER.fine(String.format("Parsing query '%s'", query));
        net.sf.jsqlparser.statement.Statement st = CCJSqlParserUtil.parse(query);
        if(st instanceof Select) {
            Select sl = (Select) st;
            SelectBody sb = sl.getSelectBody();

            List<String> columns = new ArrayList<>();
            List<String> tables = new ArrayList<>();
            List<String> filters = new ArrayList<>();

            final List<JSQLParserException> errors = new ArrayList<>();
            SelectVisitor sv = new SelectVisitorAdapter() {

                public void visit(PlainSelect plainSelect) {
                    plainSelect.getSelectItems().forEach((item) -> {
                        // TODO implement expressions and functions
                        columns.add(item.toString().replace("\"", ""));
                    });
                    FromItem fromTables = plainSelect.getFromItem();
                    FromItemVisitor fv = new FromItemVisitorAdapter() {

                        public void	visit(SubJoin subjoin) {
                            errors.add(new JSQLParserException("JOIN queries aren't supported."));
                            super.visit(subjoin);
                        }

                        public void	visit(SubSelect subSelect) {
                            errors.add(new JSQLParserException("Subqueries queries aren't supported."));
                            super.visit(subSelect);
                        }

                        public void	visit(Table tableName) {
                            ParsedSQL.LOGGER.fine(String.format("Getting table '%s'", tableName));
                            if(tableName != null)
                                tables.add(tableName.toString().replace("\"", "")
                            );
                            super.visit(tableName);
                        }

                    };
                    if(fromTables != null)
                        fromTables.accept(fv);

                    // TODO WHERE expressions

                    Expression where = plainSelect.getWhere();
                    ExpressionVisitor ev = new ExpressionVisitorAdapter() {

                        public void visit(Between expr) {
                            expr.getLeftExpression().accept(this);
                            expr.getBetweenExpressionStart().accept(this);
                            expr.getBetweenExpressionEnd().accept(this);
                        }

                        public void visit(EqualsTo expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }

                        public void visit(GreaterThan expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }


                        public void visit(GreaterThanEquals expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }

                        @Override
                        public void visit(MinorThan expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }

                        @Override
                        public void visit(MinorThanEquals expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }

                        @Override
                        public void visit(NotEqualsTo expr) {
                            System.out.println(expr);
                            visitBinaryExpression(expr);
                        }

                        @Override
                        public void visit(Column column) {
                            System.out.println(column);
                        }


                        public void visit(InExpression expr) {
                            if (expr.getLeftExpression() != null) {
                                expr.getLeftExpression().accept(this);
                            } else if (expr.getLeftItemsList() != null) {
                                expr.getLeftItemsList().accept(this);
                            }
                            if (expr.getRightExpression() != null) {
                                expr.getRightExpression().accept(this);
                            } else if (expr.getRightItemsList() != null) {
                                expr.getRightItemsList().accept(this);
                            } else {
                                expr.getMultiExpressionList().accept(this);
                            }
                        }

                    };
                    if(where != null)
                        where.accept(ev);
                    super.visit(plainSelect);
                }
            };
            sb.accept(sv);
            if(errors.size()>0) {
                throw errors.get(0);
            }
            return new ParsedSQL(columns, tables, filters);
        }
        else {
            throw new JSQLParserException("Only SELECT SQL statements are supported.");
        }
    }


}
