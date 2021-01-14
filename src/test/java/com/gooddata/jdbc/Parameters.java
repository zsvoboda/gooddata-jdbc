package com.gooddata.jdbc;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.logging.Logger;


public class Parameters {

    private final static Logger LOGGER = Logger.getLogger(Parameters.class.getName());

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    private String host;
    private String username;
    private String password;

    public String getWorkspace() {
        return workspace;
    }

    public void setWorkspace(String workspace) {
        this.workspace = workspace;
    }

    private String workspace;

    public Parameters() {
        try {
            readConfig();
        }
        catch (IOException | ParseException | org.json.simple.parser.ParseException e) {
            System.err.println("Can't read the ~/.gooddata configuration file.");
        }
    }

    public void readConfig() throws IOException, ParseException, org.json.simple.parser.ParseException {
        JSONParser parser = new JSONParser();
        Reader reader = new FileReader(String.format("%s/.gooddata", System.getProperty("user.home")));
        JSONObject jsonObject = (JSONObject) parser.parse(reader);
        this.host = (String) jsonObject.get("host");
        this.username = (String) jsonObject.get("username");
        this.password = (String) jsonObject.get("password");
        this.workspace = (String) jsonObject.get("workspace");
    }

}
