package com.mercari.solution.util;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SolrUtil {

    public static Document parseSchema(final JsonObject jsonObject) {
        final Document document = createSchemaXML("content");
        final Element root = document.getDocumentElement();

        // fields
        if(jsonObject.has("fields") && jsonObject.get("fields").isJsonArray()) {
            final Element fields = document.createElement("fields");
            for(final JsonElement field : jsonObject.getAsJsonArray("fields")) {
                final JsonObject value = field.getAsJsonObject();

                final Element fieldElement;
                if(value.has("tag")) {
                    fieldElement = document.createElement(value.getAsJsonPrimitive("tag").getAsString());
                } else {
                    fieldElement = document.createElement("field");
                }
                for(final Map.Entry<String, JsonElement> entry : value.entrySet()) {
                    if("tag".equals(entry.getKey())) {
                        continue;
                    }
                    fieldElement.setAttribute(entry.getKey(), entry.getValue().getAsString());
                }
            }
            root.appendChild(fields);
        }

        // uniqueKey
        if(jsonObject.has("uniqueKey") && jsonObject.get("uniqueKey").isJsonPrimitive()) {
            final Element uniqueKey = document.createElement("uniqueKey");
            uniqueKey.setTextContent(jsonObject.get("uniqueKey").getAsString());
            root.appendChild(uniqueKey);
        }

        // types
        if(jsonObject.has("types") && jsonObject.get("types").isJsonArray()) {
            final Element types = document.createElement("types");
            for(final JsonElement field : jsonObject.getAsJsonArray("types")) {
                final JsonObject value = field.getAsJsonObject();

                final Element typeElement;
                if(value.has("tag")) {
                    typeElement = document.createElement(value.getAsJsonPrimitive("tag").getAsString());
                } else {
                    typeElement = document.createElement("fieldType");
                }
                for(final Map.Entry<String, JsonElement> entry : value.entrySet()) {
                    if("tag".equals(entry.getKey())) {
                        continue;
                    }
                    if(entry.getValue().isJsonPrimitive()) {
                        typeElement.setAttribute(entry.getKey(), entry.getValue().getAsString());
                    } else if(entry.getValue().isJsonObject()) {
                        final JsonObject child = entry.getValue().getAsJsonObject();
                    }
                }
            }
            root.appendChild(types);
        }

        if(jsonObject.has("copyFields")) {

        }

        return document;
    }

    public static Document createSchemaXML(final String coreName) {
        final Document document = XmlUtil.createDocument("schema");
        ((Element)document.getFirstChild()).setAttribute("name", coreName);
        ((Element)document.getFirstChild()).setAttribute("version", "1.6");
        return document;
    }

    public static void setDefaultSchemaTypes(final Document document, final Element types) {

        // string
        final Element stringType = document.createElement("fieldType");
        stringType.setAttribute("name", "string");
        stringType.setAttribute("class", "solr.StrField");
        stringType.setAttribute("sortMissingLast", "true");
        types.appendChild(stringType);
        // boolean
        final Element booleanType = document.createElement("fieldType");
        booleanType.setAttribute("name", "boolean");
        booleanType.setAttribute("class", "solr.BoolField");
        booleanType.setAttribute("sortMissingLast", "true");
        types.appendChild(booleanType);
        // int
        final Element intType = document.createElement("fieldType");
        intType.setAttribute("name", "int");
        intType.setAttribute("class", "solr.IntPointField");
        intType.setAttribute("sortMissingLast", "true");
        types.appendChild(intType);
        // long
        final Element longType = document.createElement("fieldType");
        longType.setAttribute("name", "long");
        longType.setAttribute("class", "solr.LongPointField");
        longType.setAttribute("sortMissingLast", "true");
        types.appendChild(longType);
        // float
        final Element floatType = document.createElement("fieldType");
        floatType.setAttribute("name", "float");
        floatType.setAttribute("class", "solr.FloatPointField");
        floatType.setAttribute("sortMissingLast", "true");
        types.appendChild(floatType);
        // double
        final Element doubleType = document.createElement("fieldType");
        doubleType.setAttribute("name", "double");
        doubleType.setAttribute("class", "solr.DoublePointField");
        doubleType.setAttribute("sortMissingLast", "true");
        types.appendChild(doubleType);
        // date
        final Element dateType = document.createElement("fieldType");
        dateType.setAttribute("name", "date");
        dateType.setAttribute("class", "solr.DatePointField");
        dateType.setAttribute("sortMissingLast", "true");
        types.appendChild(dateType);

        // text_ja
        final Element textJaType = document.createElement("fieldType");
        textJaType.setAttribute("name", "textja");
        textJaType.setAttribute("class", "solr.TextField");
        textJaType.setAttribute("positionIncrementGap", "100");
        {
            final Element analyzer = document.createElement("analyzer");
            {
                final Element tokenizer = document.createElement("tokenizer");
                tokenizer.setAttribute("class", "solr.JapaneseTokenizerFactory");
                tokenizer.setAttribute("mode", "search");
                analyzer.appendChild(tokenizer);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.JapaneseBaseFormFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.CJKWidthFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.LowerCaseFilterFactory");
                analyzer.appendChild(filter);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.JapaneseKatakanaStemFilterFactory");
                filter.setAttribute("minimumLength", "2");
                analyzer.appendChild(filter);
            }
            textJaType.appendChild(analyzer);
        }
        types.appendChild(textJaType);

        // text_bi
        final Element textBiType = document.createElement("fieldType");
        textBiType.setAttribute("name", "textbi");
        textBiType.setAttribute("class", "solr.TextField");
        textBiType.setAttribute("positionIncrementGap", "100");
        {
            final Element analyzer = document.createElement("analyzer");
            {
                final Element tokenizer = document.createElement("tokenizer");
                tokenizer.setAttribute("class", "solr.NGramTokenizerFactory");
                tokenizer.setAttribute("minGramSize", "2");
                tokenizer.setAttribute("maxGramSize", "2");
                analyzer.appendChild(tokenizer);
            }
            {
                final Element filter = document.createElement("filter");
                filter.setAttribute("class", "solr.LowerCaseFilterFactory");
                analyzer.appendChild(filter);
            }
            textBiType.appendChild(analyzer);
        }
        types.appendChild(textBiType);


        // random
        final Element randomType = document.createElement("fieldType");
        randomType.setAttribute("name", "random");
        randomType.setAttribute("class", "solr.RandomSortField");
        types.appendChild(randomType);

        // ignore
        final Element ignoreType = document.createElement("fieldType");
        ignoreType.setAttribute("name", "ignored");
        ignoreType.setAttribute("indexed", "false");
        ignoreType.setAttribute("stored", "false");
        ignoreType.setAttribute("class", "solr.StrField");
        types.appendChild(ignoreType);

    }

    public static List<String> getFieldNames(final String schemaString) {

        try(final InputStream is = new ByteArrayInputStream(schemaString.getBytes())) {
            final DocumentBuilder documentBuilder = DocumentBuilderFactory
                    .newInstance()
                    .newDocumentBuilder();
            final Document document = documentBuilder.parse(is);

            final NodeList nodeList = document.getElementsByTagName("fields");
            if(nodeList.getLength() == 0) {
                return new ArrayList<>();
            }

            final Node fields = nodeList.item(0);
            final NodeList fs = fields.getChildNodes();

            final List<String> fieldNames = new ArrayList<>();
            for(int i=0; i<fs.getLength(); i++) {
                final Node node = fs.item(i);
                if(!"field".equals(node.getNodeName())) {
                    continue;
                }
                if(node.getAttributes().getNamedItem("name") == null) {
                    continue;
                }
                fieldNames.add(node.getAttributes().getNamedItem("name").getNodeValue());
            }
            return fieldNames;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
