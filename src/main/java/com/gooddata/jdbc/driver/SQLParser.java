package com.gooddata.jdbc.driver;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import org.apache.tools.ant.types.resources.Union;

import java.util.ArrayList;
import java.util.List;

public class SQLParser {

    public static class ParsedSQL {

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
                        }

                        public void	visit(SubSelect subSelect) {
                            errors.add(new JSQLParserException("Subqueries queries aren't supported."));
                        }

                        public void	visit(Table tableName) {
                            tables.add(tableName.toString().replace("\"", ""));
                        }

                    };
                    fromTables.accept(fv);

                    // TODO WHERE expressions
                    /**
                    Expression where = plainSelect.getWhere();
                    ExpressionVisitor ev = new ExpressionVisitorAdapter() {};
                    where.accept(ev);
                     */
                }

                public void visit(Union u) {
                    errors.add(new JSQLParserException("UNIONs queries aren't supported."));
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
