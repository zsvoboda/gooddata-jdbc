package com.gooddata.jdbc.util;

import com.gooddata.jdbc.driver.SQLParser;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.sql.SQLException;

public class TestDataTypeParser {

    @Test
    public void testParseBool() throws SQLException {
        assert(DataTypeParser.parseBoolean("T"));
        assert(DataTypeParser.parseBoolean("t"));
        assert(DataTypeParser.parseBoolean("tRue"));
        assert(DataTypeParser.parseBoolean("TRUE"));
        assert(DataTypeParser.parseBoolean("true"));
        assert(DataTypeParser.parseBoolean("1"));

        assert(!DataTypeParser.parseBoolean("F"));
        assert(!DataTypeParser.parseBoolean("f"));
        assert(!DataTypeParser.parseBoolean("fAlse"));
        assert(!DataTypeParser.parseBoolean("FALSE"));
        assert(!DataTypeParser.parseBoolean("false"));
        assert(!DataTypeParser.parseBoolean("0"));

        assert(DataTypeParser.parseShort("1") == 1);
        assert(DataTypeParser.parseInt("3") == 3);
        assert(DataTypeParser.parseLong("123456789") == 123456789);

        assert(DataTypeParser.parseFloat("1.23") == 1.23f);
        assert(DataTypeParser.parseDouble("3.45678932456") == 3.45678932456d);
        assert(DataTypeParser.parseBigDecimal("23456").equals(new BigDecimal("23456")));
    }

    @Test(expectedExceptions = { SQLException.class })
    public void testParseBoolException() throws SQLException {
        assert(DataTypeParser.parseBoolean("1"));
        assert(DataTypeParser.parseBoolean("foo"));
        assert(DataTypeParser.parseBoolean("tr"));
        assert(DataTypeParser.parseInt("tr") == 1);
        assert(DataTypeParser.parseInt("1.2") == 1.2);
    }

    @Test
    public void testParseDatatype() throws SQLException {
        assert(SQLParser.parseSqlDatatype("DATETIME").getName().equalsIgnoreCase("DATETIME"));
        assert(SQLParser.parseSqlDatatype("VARCHAR(32)").getName().equalsIgnoreCase("VARCHAR"));
        assert(SQLParser.parseSqlDatatype("Varchar(32)").getName().equalsIgnoreCase("VARCHAR"));
        assert(SQLParser.parseSqlDatatype("varchar(32)").getName().equalsIgnoreCase("VARCHAR"));
        assert(SQLParser.parseSqlDatatype("varchar(32)").getName().equalsIgnoreCase("VARCHAR"));
        assert(SQLParser.parseSqlDatatype("varChar(32)").getName().equalsIgnoreCase("VARCHAR"));
        assert(SQLParser.parseSqlDatatype("VARCHAR (32)").getSize() == 32);
        assert(SQLParser.parseSqlDatatype("VARCHAR ( 32)").getSize() == 32);
        assert(SQLParser.parseSqlDatatype("VARCHAR ( 32 )").getSize() == 32);
        assert(SQLParser.parseSqlDatatype("VARCHAR( 32 )").getSize() == 32);
        assert(SQLParser.parseSqlDatatype("VARCHAR(32 )").getSize() == 32);
        assert(SQLParser.parseSqlDatatype(" VARCHAR(32 ) ").getSize() == 32);
        assert(SQLParser.parseSqlDatatype("DECIMAL(13,2)").getName().equalsIgnoreCase("DECIMAL"));
        assert(SQLParser.parseSqlDatatype("DECIMAL (13,2)").getSize() == 13);
        assert(SQLParser.parseSqlDatatype("DECIMAL ( 13,2) ").getPrecision() == 2);
        assert(SQLParser.parseSqlDatatype("DECIMAL ( 13 , 2 ) ").getPrecision() == 2);
        assert(SQLParser.parseSqlDatatype("DECIMAL ( 13 , 2) ").getPrecision() == 2);
    }

}
