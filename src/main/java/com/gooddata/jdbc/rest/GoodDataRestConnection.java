package com.gooddata.jdbc.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gooddata.jdbc.driver.Catalog;
import com.gooddata.sdk.model.project.Project;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GoodDataRestConnection {

    private RestTemplate gdRestTemplate;
    private Project workspace;

    private static final String ELEMENT_LABEL_TO_URI = "{\"elementLabelToUri\":[{\"mode\": " +
            "\"EXACT\",\"labelUri\":\"\",\"patterns\":[]}]}";

    public GoodDataRestConnection(RestTemplate gdRestTemplate, Project workspace) {
        this.gdRestTemplate = gdRestTemplate;
        this.workspace = workspace;
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
