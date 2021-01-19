package com.gooddata.jdbc.catalog;

import com.gooddata.sdk.common.collections.PageBrowser;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.project.ProjectService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Schema {

    private final String schemaName;
    private final String schemaUri;

    private Schema(String schemaName, String schemaUri) {
        this.schemaName = schemaName;
        this.schemaUri = schemaUri;
    }

    public static List<Schema> populateSchemas(GoodData gd) {
        ProjectService projectService = gd.getProjectService();
        List<Project> workspaces = new ArrayList<>();
        PageBrowser<Project> list = projectService.listProjects();
        do {
            workspaces.addAll(list.allItemsStream().collect(Collectors.toList()));
        } while(list.getNextPage() != null);
        return workspaces.stream()
                .map( e -> new Schema(e.getTitle(), e.getUri())).collect(Collectors.toList());
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getSchemaUri() {
        return schemaUri;
    }
}
