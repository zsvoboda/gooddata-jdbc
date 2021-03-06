package com.gooddata.jdbc.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gooddata.jdbc.catalog.Catalog;
import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.parser.MaqlParser;
import com.gooddata.jdbc.util.TextUtil;
import com.gooddata.sdk.model.executeafm.ObjQualifier;
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.*;

public class GoodDataRestConnection {

    private final RestTemplate gdRestTemplate;
    private final Project workspace;

    private static final String ELEMENT_LABEL_TO_URI = "{\"elementLabelToUri\":[{\"mode\": " +
            "\"EXACT\",\"labelUri\":\"\",\"patterns\":[]}]}";

    public GoodDataRestConnection(RestTemplate gdRestTemplate, Project workspace) {
        this.gdRestTemplate = gdRestTemplate;
        this.workspace = workspace;
    }

    public void updateMetric(Metric m, String definition) throws SQLException {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode requestObj = mapper.valueToTree(m);
        ObjectNode contentNode = (ObjectNode) requestObj.get("metric").get("content");
        contentNode.put("expression", definition);
        contentNode.remove("tree");
        HttpEntity<JsonNode> request = new HttpEntity<>(requestObj, headers);
        String url = m.getUri();
        ResponseEntity<JsonNode> response = this.gdRestTemplate.postForEntity(url, request,
                JsonNode.class);
        if (response.getStatusCode() != HttpStatus.OK) {
            throw new SQLException(String.format("ALTER METRIC '%s' failed.", definition));
        }

    }

    public String replaceMaqlTitlesWithUris(MaqlParser.ParsedCreateMetricStatement parsedMaqlCreate,
                                            Catalog catalog)
            throws Catalog.CatalogEntryNotFoundException, Catalog.DuplicateCatalogEntryException,
            TextUtil.InvalidFormatException {
        String maqlDefinition = parsedMaqlCreate.getMetricMaqlDefinition();
        // Replace all metric titles in the MAQL definition with their URIs
        for(String metricFactAttribute: parsedMaqlCreate.getLdmObjectTitles()) {
            //lookup attribute in LDM
            CatalogEntry ldmObj = catalog.findByName(metricFactAttribute);
            String replaceWhat = String.format("\"%s\"", metricFactAttribute);
            maqlDefinition = maqlDefinition.replace(
                    replaceWhat,
                    String.format("[%s]", ldmObj.getUri()));
        }
        // Replace attribute elements in the MAQL definition with their URIs
        // This lookup contains attribute URI for every attribute element
        Map<String,String> elementToAttribute = parsedMaqlCreate
                .getAttributeElementToAttributeNameLookup();
        for(String value: parsedMaqlCreate.getAttributeElementValues()) {
            String attributeName = elementToAttribute.get(value);
            if(attributeName == null)
                throw new Catalog.CatalogEntryNotFoundException(
                        "The value '%s' can't be associated with any attribute.");
            //lookup display form in AFM
            CatalogEntry attribute = catalog.findByName(attributeName);
            String replaceWhat = String.format("'%s'", value);
            Map<String, String> lookup = lookupAttributeElements(attribute.getDefaultDisplayForm().getUri(),
                            Collections.singletonList(value));
            if(lookup == null || lookup.size() == 0)
                throw new Catalog.CatalogEntryNotFoundException(
                        String.format("The value '%s' can't be mapped to any element URI.", value));
            String elementUri = lookup.get(value);
            if(elementUri == null || elementUri.length() == 0)
                throw new Catalog.CatalogEntryNotFoundException(
                        "The value '%s' doesn't exist.");
            String replaceWith = String.format("[%s]", elementUri);
            maqlDefinition = maqlDefinition.replace(replaceWhat, replaceWith);
        }
        return maqlDefinition;
    }

    // Attribute elements cache
    private final Map<String, String> attributeElementsCache = new HashMap<>();

    /**
     * Get AttributeElement by uri
     *
     * @param attributeElementUri AttributeElement uri
     * @return value AttributeElement value
     */
    public String getAttributeElementText(String attributeElementUri)
            throws Catalog.CatalogEntryNotFoundException {
        if(this.attributeElementsCache.containsKey(attributeElementUri))
            return this.attributeElementsCache.get(attributeElementUri);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<JsonNode> response = this.gdRestTemplate.getForEntity(attributeElementUri,
                JsonNode.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            String value = Objects.requireNonNull(response.getBody())
                    .get("attributeElements")
                    .get("elements")
                    .get(0)
                    .get("title")
                    .textValue();
            this.attributeElementsCache.put(attributeElementUri, value);
            return value;
        }
        else {
            throw new Catalog.CatalogEntryNotFoundException(
                    String.format("Getting AttributeElement for uri '%s' failed.", attributeElementUri));
        }
    }


    /**
     * Lookups AttributeDisplayForm URIs for values
     *
     * @param displayFormUri AttributeDisplayForm uri
     * @param values         values
     */
    public Map<String, String> lookupAttributeElements(String displayFormUri,
                                                       List<String> values)
            throws Catalog.CatalogEntryNotFoundException {
        try {

            Map<String, String> elementUris = new HashMap<>();

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode requestObj = mapper.readTree(ELEMENT_LABEL_TO_URI);
            ObjectNode rootNode = (ObjectNode) requestObj.get("elementLabelToUri").get(0);
            rootNode.put("labelUri", displayFormUri);
            ArrayNode valuesArray = (ArrayNode) requestObj.get("elementLabelToUri").get(0).get("patterns");
            for (String value : values) {
                valuesArray.add(value);
            }
            HttpEntity<JsonNode> request = new HttpEntity<>(requestObj, headers);
            String url = String.format("%s/labels", this.workspace.getMetadataUri());
            ResponseEntity<JsonNode> response = this.gdRestTemplate.postForEntity(url, request, JsonNode.class);
            if (response.getStatusCode() == HttpStatus.OK) {
                ArrayNode results = (ArrayNode) Objects.requireNonNull(response.getBody())
                        .get("elementLabelUri").get(0).get("result");
                for (JsonNode result : results) {
                    ArrayNode elementLabels = (ArrayNode) result.get("elementLabels");
                    for(JsonNode row: elementLabels) {
                        elementUris.put(row.get("elementLabel").textValue(),
                                row.get("uri").textValue());
                    }
                }
                return elementUris;
            }
            else {
                throw new Catalog.CatalogEntryNotFoundException(
                        String.format("AttributeElements lookup failed for uri '%s'", displayFormUri));
            }
        } catch (IOException e) {
            throw new Catalog.CatalogEntryNotFoundException(e);
        }
    }

    public static class Variable implements ObjQualifier, Serializable {

        public Variable(String uri, String identifier, String title, String expression) {
            this.uri = uri;
            this.identifier = identifier;
            this.title = title;
            this.expression = expression;
        }

        public String getUri() {
            return uri;
        }

        public String getIdentifier() {
            return identifier;
        }

        public String getExpression() {
            return expression;
        }

        public String getTitle() {
            return title;
        }

        private final String uri;
        private final String title;
        private final String identifier;
        private final String expression;
    }

    /**
     * Get variable
     * @param  requestUri variable URI
     * @return variable
     */
    public CatalogEntry getVariable(String requestUri, String expression) throws Catalog.CatalogEntryNotFoundException {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<JsonNode> response = this.gdRestTemplate.getForEntity(requestUri,
                JsonNode.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            JsonNode meta = Objects.requireNonNull(response.getBody())
                    .get("prompt")
                    .get("meta");
            JsonNode content = Objects.requireNonNull(response.getBody())
                    .get("prompt")
                    .get("content");
            String uri = meta.get("uri").textValue();
            String title = meta.get("title").textValue();
            String identifier = meta.get("identifier").textValue();
            String type = content.get("type").textValue();
            return new CatalogEntry(uri, title, "prompt", identifier,
                    new Variable(uri, identifier, title, expression), type,0,0);
        }
        else {
            throw new Catalog.CatalogEntryNotFoundException(
                    String.format("Getting variable for requestUri '%s' failed.", requestUri));
        }
    }

    /**
     * List variables
     * @param  workspaceUri workspace ID
     * @return list of variables
     */
    public List<CatalogEntry> getVariables(String workspaceUri)
            throws SQLException {
        try {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String uri = String.format("%s/variables/search", workspaceUri)
                .replace("/projects/","/md/");
        ObjectMapper mapper = new ObjectMapper();
        JsonNode requestObj = mapper.readTree("{\"variablesSearch\": { \"variables\": [], \"context\": []}}");
        ResponseEntity<JsonNode> response = this.gdRestTemplate.postForEntity(uri, requestObj,
                JsonNode.class);
        if (response.getStatusCode() == HttpStatus.OK) {
            List<CatalogEntry> variableEntries = new ArrayList<>();
            ArrayNode variables = (ArrayNode) Objects.requireNonNull(response.getBody()).get("variables");
            for (JsonNode variable : variables) {
                String variableExpression = variable.get("expression").textValue();
                String variableUri = variable.get("prompt").textValue();
                variableEntries.add(this.getVariable(variableUri, variableExpression));
            }
            return variableEntries;
        }
        else {
            throw new SQLException(
                    "Getting variables for uri '%s' failed.");
        }
        } catch (JsonProcessingException | Catalog.CatalogEntryNotFoundException e) {
            throw new SQLException(e);
        }
    }


}
