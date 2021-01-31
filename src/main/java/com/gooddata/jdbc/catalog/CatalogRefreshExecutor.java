package com.gooddata.jdbc.catalog;

import java.sql.SQLException;

/**
 * Thread for asynchronous catalog refresh
 */
public class CatalogRefreshExecutor extends Thread {

    private final Catalog catalog;
    private final String workspaceUri;

    public CatalogRefreshExecutor(Catalog catalog, String workspaceUri) {
        this.catalog = catalog;
        this.workspaceUri = workspaceUri;
    }

    @Override
    public void run() {
        try {
            catalog.populateSync(this.workspaceUri);
        } catch (SQLException e) {
            // TBD better error handling
            e.printStackTrace();
        }
    }
}
