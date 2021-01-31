package com.gooddata.jdbc.catalog;

import com.gooddata.jdbc.rest.GoodDataRestConnection;
import com.gooddata.sdk.service.GoodData;

import java.sql.SQLException;

/**
 * Thread for asynchronous catalog refresh
 */
public class CatalogRefreshExecutor extends Thread {

    private final Catalog catalog;
    private final GoodData gd;
    private final GoodDataRestConnection gdRest;
    private final String workspaceUri;

    public CatalogRefreshExecutor(Catalog catalog, GoodData gd, GoodDataRestConnection gdRest, String workspaceUri) {
        this.catalog = catalog;
        this.gd = gd;
        this.gdRest = gdRest;
        this.workspaceUri = workspaceUri;
    }

    @Override
    public void run() {
        try {
            catalog.populateSync(this.gd, this.gdRest, this.workspaceUri);
        } catch (SQLException e) {
            // TBD better error handling
            e.printStackTrace();
        }
    }
}
