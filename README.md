# GoodData workspace JDBC driver 
JDBC driver for accessing GoodData workspace data using the 
standard JDBC protocol.

![](https://github.com/zsvoboda/gooddata-jdbc/wiki/images/dbeaver.png)

## Getting Started

You need just [this JAR file](https://github.com/zsvoboda/gooddata-jdbc/wiki/files/gooddata-jdbc-0.6.jar) on your ```CLASSPATH``` 

The JDBC driver class is 

```com.gooddata.jdbc.driver.AfmDriver```

and it expects the following JDBC URL format 

``` jdbc:gd://<your-gooddata-domain-name>/gdc/projects/<your-gooddata-project-id> ```

### Supported features
- You don't use FROM clause. Just list of columns in the ```SELECT <column-list> ``` 
  and ```WHERE <conditions> ```
- Columns are quoted in double-quotes (```"```)
- Textual values are quoted in single-quotes (```'```)
- SELECT column list 
    - only supports plain list of attributes and metrics (no expressions or functions)
    - supports datatype specification using ```"Revenue::DECIMAL(13,2)"```
- WHERE clause
    - only supports ```AND``` logical operators 
    - supports ``` =,<>, IN, NOT IN ``` operators for attributes
    - supports ``` =,<>, >, <, >=, <=, BETWEEN, NOT BETWEEN ``` operators for metrics
    - supports simple expressions like ```(2+5)*3```
- ORDER BY - not yet supported
- LIMIT, OFFSET - not yet supported
- MAQL
    - supports ```CREATE METRIC <name> AS <maql>```
    - supports ```ALTER METRIC <name> AS <maql>```
    - supports ```DROP METRIC <name>```
  

### Tested with
- [DBeaver](https://dbeaver.io/)
- [IntelliJ, DataGrip, and other JetBrains tools](https://www.jetbrains.com/)
- [Squirrel SQL](https://http://squirrel-sql.sourceforge.net/) doesn't work. Let me know if you have any idea why.

## License
[Plain MIT license](LICENSE)

## Let me know
Submit github [issue](https://github.com/zsvoboda/gooddata-jdbc/issues). 