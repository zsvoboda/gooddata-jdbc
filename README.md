# GoodData workspace JDBC driver 
JDBC driver for accessing GoodData workspace data using the 
standard JDBC protocol.

![](https://github.com/zsvoboda/gooddata-jdbc/wiki/images/dbeaver.png)

## Getting Started

You need just [this JAR file](https://github.com/zsvoboda/gooddata-jdbc/releases/download/0.72/gooddata-jdbc-0.72.jar) on your ```CLASSPATH``` 

The JDBC driver class is 

```com.gooddata.jdbc.driver.AfmDriver```

and it expects the following JDBC URL format 

``` jdbc:gd://<your-gooddata-domain-name>/gdc/projects/<your-gooddata-project-id> ```

### Supported features
- You don't use FROM clause. Just list of columns in the ```SELECT <column-list> ``` 
  and ```WHERE <conditions> ```
- Columns (attributes and metrics) are quoted in double-quotes (```"Revenue"```)
  - columns can be referenced by name or by URI (```[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/275]```)   
- Textual values are quoted in single-quotes (```'Home'```)
- SELECT column list 
    - only supports plain list of attributes and metrics (no expressions or functions)
    - supports datatype specification using ```"Revenue::DECIMAL(13,2)"```
- WHERE clause
    - only supports ```AND``` logical operators 
    - supports ``` =,<>, IN, NOT IN ``` operators for attributes
    - supports ``` =,<>, >, <, >=, <=, BETWEEN, NOT BETWEEN ``` operators for metrics
    - supports simple expressions like ```(2+5)*3```
- ORDER BY - standard support (e.g. ```ORDER BY 1 ASC, 2 DESC``` or ```ORDER BY "Product" ASC, "Product Category" DESC``` )
- LIMIT, OFFSET standard support (e.g. ```LIMIT 100 OFFSET 35```)
- Prepared statements (Connection.prepareStatement + statement.setXY)
    - This is a fake implementation that does the same as execution of regular statement with parameters substitution. 
      No performance benefits.   
- MAQL
    - supports ```CREATE METRIC <name> AS <maql>```
    - supports ```ALTER METRIC <name> AS <maql>```
    - supports ```DROP METRIC <name>```
    - supports ```DESCRIBE METRIC <name>```
    - make sure you quote all identifiers in double quotes

### Example
```
CREATE METRIC "Total Revenue by State" AS SELECT "Revenue" BY "Customer State" ALL OTHER;

SELECT "Product Category", "Customer State", "Revenue", "Total Revenue by State" 
WHERE "Customer State" IN ('CA','MA');

SELECT "Product Category", "Product", "# of Orders" ORDER BY 3 ASC LIMIT 10 OFFSET 3;

DECRIBE METRIC "Total Revenue by State";

DROP METRIC "Total Revenue by State";
```

### Tested with
- [DBeaver](https://dbeaver.io/)
- [IntelliJ, DataGrip, and other JetBrains tools](https://www.jetbrains.com/)
- [Squirrel SQL](https://http://squirrel-sql.sourceforge.net/) doesn't work. Let me know if you have any idea why.

## License
[MIT license](LICENSE)

## Tutorials and articles

- [Motivation: SQL and aggregated data: is there a better way?](https://github.com/zsvoboda/gooddata-jdbc/wiki/SQL-and-aggregated-data:-is-there-a-better-way%3F)
- [DBeaver JDBC tutorial](https://github.com/zsvoboda/gooddata-jdbc/wiki/GoodData-metrics-tutorial)

## Let me know
Submit github [issue](https://github.com/zsvoboda/gooddata-jdbc/issues). 