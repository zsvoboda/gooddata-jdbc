# gooddata-jdbc
JDBC driver for accessing GoodData workspace data via 
JDBC protocol.

Tests read configuration from _~/.gooddata_ file located in 
your HOME directory.

```
{
    "host": "<your-gooddata-hostname>",
    "username": "<gooddata-username>",
    "password": "<gooddata-password>",
    "workspace":"<workspace-id>"
}
```