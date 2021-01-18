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
import com.gooddata.sdk.model.md.Metric;
import com.gooddata.sdk.model.project.Project;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoodDataRestConnection {

    private RestTemplate gdRestTemplate;
    private Project workspace;

    private static final String ELEMENT_LABEL_TO_URI = "{\"elementLabelToUri\":[{\"mode\": " +
            "\"EXACT\",\"labelUri\":\"\",\"patterns\":[]}]}";

    private static final String METRIC = "{\"metric\": {\"content\": {\"expression\": \"\", " +
            "\"format\": \"\"}, \"meta\": {\"category\": \"metric\", \"deprecated\": \"0\", " +
            "\"isProduction\": 1, \"summary\": \"\",\"title\": \"\",\"uri\" : \"\"}}}";

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
            CatalogEntry ldmObj = catalog.findMaqlColumn(metricFactAttribute);
            String replaceWhat = String.format("\"%s\"", metricFactAttribute);
            maqlDefinition = maqlDefinition.replaceAll(
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
            CatalogEntry ldmObj = catalog.findAfmColumn(attributeName);
            String replaceWhat = String.format("'%s'", value);
            Map<String, String> lookup = lookupAttributeElements(ldmObj.getUri(),
                            Collections.singletonList(value));
            if(lookup == null || lookup.size() == 0)
                throw new Catalog.CatalogEntryNotFoundException(
                        String.format("The value '%s' can't be mapped to any element URI.", value));
            String elementUri = lookup.get(value);
            if(elementUri == null || elementUri.length() == 0)
                throw new Catalog.CatalogEntryNotFoundException(
                        "The value '%s' doesn't exist.");
            String replaceWith = String.format("[%s]", elementUri);
            maqlDefinition = maqlDefinition.replaceAll(replaceWhat, replaceWith);
        }
        return maqlDefinition;
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
                ArrayNode results = (ArrayNode) response.getBody()
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
        } catch (JsonProcessingException e) {
            throw new Catalog.CatalogEntryNotFoundException(e);
        }
    }


}
