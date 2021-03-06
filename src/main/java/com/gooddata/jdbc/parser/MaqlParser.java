package com.gooddata.jdbc.parser;

import net.sf.jsqlparser.JSQLParserException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaqlParser {

    /**
     * Parsed CREATE METRIC statement
     */
    public static class ParsedCreateMetricStatement {

        /**
         * Constructor
         * @param name  MAQL metric name
         * @param metricMaqlDefinition MAQL metric definition
         * @param ldmObjectTitles titles of LDM objects that the CREATE METRIC statement uses
         * @param attributeElementValues attribute element values that the CREATE METRIC statement uses
         * @param attributeElementToAttributeNameLookup lookup that translates attribute element value to attribute name
         */
        public ParsedCreateMetricStatement(String name, String metricMaqlDefinition, List<String> ldmObjectTitles,
                                           List<String> attributeElementValues,
                                           Map<String,String> attributeElementToAttributeNameLookup) {
            this.metricMaqlDefinition = metricMaqlDefinition;
            this.ldmObjectTitles = ldmObjectTitles;
            this.attributeElementValues = attributeElementValues;
            this.name = name;
            this.attributeElementToAttributeNameLookup = attributeElementToAttributeNameLookup;
        }

        public String getMetricMaqlDefinition() {
            return metricMaqlDefinition;
        }

        public List<String> getLdmObjectTitles() {
            return ldmObjectTitles;
        }

        public List<String> getAttributeElementValues() {
            return attributeElementValues;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Map<String, String> getAttributeElementToAttributeNameLookup() {
            return attributeElementToAttributeNameLookup;
        }

        private final String metricMaqlDefinition;
        private final List<String> ldmObjectTitles;
        private final List<String> attributeElementValues;
        private final Map<String, String> attributeElementToAttributeNameLookup;

        private String name;
    }

    /**
     * Parses MQL metric text
     * @param metricName name of the metric
     * @param metricMaql metric MAQL
     * @return parser MAQL metric structure
     * @throws JSQLParserException in case of a parser error
     */
    public ParsedCreateMetricStatement parseMaql(String metricName, String metricMaql) throws JSQLParserException {
        List<String> factsMetricsOrAttributeTitles = new ArrayList<>();
        Pattern p1 = Pattern.compile("(\".*?\")+");
        Matcher m1 = p1.matcher(metricMaql);
        while (m1.find()) {
            for (int i = 1; i <= m1.groupCount(); i++) {
                factsMetricsOrAttributeTitles.add(m1.group(i).replaceAll("\"",""));
            }
        }

        Pattern p3 = Pattern.compile(
                "^\\s?.*?where\\s+(.*?)\\s?$",Pattern.CASE_INSENSITIVE);
        Matcher m3 = p3.matcher(metricMaql);
        Map<String, String> attributeElementToAttributeNameLookup = new HashMap<>();
        List<String> attributeElementValues = new ArrayList<>();
        if( m3.find()) {
            String whereClause = m3.group(1);
            Pattern p2 = Pattern.compile("(['\"].*?['\"])");
            Matcher m2 = p2.matcher(whereClause);
            String leadingAttribute = null;
            while (m2.find()) {
                for (int i = 1; i <= m1.groupCount(); i++) {
                    String attributeOrElement = m2.group(i);
                    if(attributeOrElement.startsWith("\"")) {
                        leadingAttribute = attributeOrElement.replaceAll("\"","");
                    } else {
                        String value = attributeOrElement.replaceAll("'","");
                        attributeElementValues.add(value);
                        if(leadingAttribute == null)
                            throw new JSQLParserException(String.format("Wrong WHERE syntax: '%s'. The '%s' value " +
                                    "can't be matched with any attribute.", whereClause, value));
                        attributeElementToAttributeNameLookup.put(value, leadingAttribute);
                    }
                }
            }
        }
        return new ParsedCreateMetricStatement(metricName, metricMaql,
                factsMetricsOrAttributeTitles, attributeElementValues,
                attributeElementToAttributeNameLookup);
    }

    /**
     * Parses CREATE METRIC statement or ALTER METRIC statement
     * @param maql CREATE METRIC statement text
     * @return parsed CREATE METRIC statement
     * @throws JSQLParserException syntax errors
     */
    public ParsedCreateMetricStatement parseCreateOrAlterMetric(String maql) throws JSQLParserException {
        String sqlWithNoNewlines = maql.replaceAll("\n"," ");
        Pattern p = Pattern.compile(
                "^\\s?(create|alter)\\s+metric\\s+\"(.*?)\"\\s+as\\s+(.*?)\\s?[;]?\\s?$",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sqlWithNoNewlines);
        boolean b = m.matches();
        if (!b || m.groupCount() != 3)
            throw new JSQLParserException(String.format("Wrong CREATE METRIC syntax (e.g. no quoted names of metrics or identifiers): '%s'", maql));
        String metricName = m.group(2);
        String metricMaql = m.group(3);
        return parseMaql(metricName, metricMaql);
    }

    public static class Describe {

        public Describe(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        private final String name;
        private final String type;
    }

    /**
     * Parse DROP | DESCRIBE METRIC statement
     * @param maql DROP | DESCRIBE METRIC statement text
     * @return dropped metric URI
     * @throws JSQLParserException syntax error
     */
    public Describe parseDropOrDescribeMetric(String maql) throws JSQLParserException {
        String sqlWithNoNewlines = maql.replaceAll("\n"," ");
        Pattern p = Pattern.compile(
                "^\\s?(drop|describe)\\s+(metric|variable)\\s+\"(.*?)\"\\s?[;]?\\s?$",
                Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(sqlWithNoNewlines);
        boolean b = m.matches();
        if (!b || m.groupCount() != 3)
            throw new JSQLParserException(String.format("Wrong DROP METRIC syntax (e.g. no quoted names of " +
                    "metrics or identifiers): '%s'", maql));
        return new Describe(m.group(3), m.group(2));
    }


}
