package com.gooddata.jdbc.rest;

import com.gooddata.jdbc.util.Parameters;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.GoodData;
import com.gooddata.sdk.service.GoodDataEndpoint;
import com.gooddata.sdk.service.GoodDataSettings;
import com.gooddata.sdk.service.httpcomponents.LoginPasswordGoodDataRestProvider;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.Test;

import java.sql.SQLException;

public class TestRestConnection {

    private final GoodDataRestConnection gdRestConnection;
    private final String workspaceUri;

    public TestRestConnection() {
        Parameters p = new Parameters();
        this.workspaceUri = String.format("/gdc/projects/%s",p.getWorkspace());
        GoodData gd = new GoodData(p.getHost(), p.getUsername(), p.getPassword());
        LoginPasswordGoodDataRestProvider lp = new LoginPasswordGoodDataRestProvider(
                new GoodDataEndpoint(p.getHost(), GoodDataEndpoint.PORT, GoodDataEndpoint.PROTOCOL),
                new GoodDataSettings(),
                p.getUsername(),
                p.getPassword());
        RestTemplate gdRestTemplate = lp.getRestTemplate();
        Project workspace = gd.getProjectService().getProjectByUri(
                String.format("/gdc/projects/%s",p.getWorkspace())
        );
        this.gdRestConnection = new GoodDataRestConnection(gdRestTemplate, workspace);
    }

    @Test
    public void testVersion() throws SQLException {
        this.gdRestConnection.getVariables(this.workspaceUri);
    }

}
