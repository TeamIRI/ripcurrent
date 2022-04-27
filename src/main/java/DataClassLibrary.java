/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Read from an existing IRI data class library file.
 *
 * Contributors:
 *     devonk
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class DataClassLibrary {
    Map<Map<String, Rule>, DataMatcher> dataMatcherMap = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(DataClassLibrary.class);

    DataClassLibrary(String filePath, Map<String, Rule> rules) {
        try {
            File dataClassLibraryFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(dataClassLibraryFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("dataClasses");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String[] defaultRule;
                    try {
                        defaultRule = eElement.getChildNodes().item(0).getNextSibling().getAttributes().getNamedItem("href").getNodeValue().split("#");
                        if (rules.get(defaultRule[1]) != null) {
                            defaultRule[1] = rules.get(defaultRule[1]).getRule();
                        } else { // Default
                           continue;
                        }
                    } catch (NullPointerException e) {
                        continue;
                    }
                    NodeList matchersList = eElement.getElementsByTagName("matchers");
                    for (int temp2 = 0; temp2 < matchersList.getLength(); temp2++) {
                        Node nNode2 = matchersList.item(temp2);
                        HashMap<String, Rule> ruleMap = new HashMap<>();
                        ruleMap.put(((Element) nNode).getAttribute("name"), new Rule(rules.get(defaultRule[1]).getType(), defaultRule[1]));
                        if (nNode2.getAttributes().getNamedItem("type") != null && nNode2.getAttributes().getNamedItem("type").getNodeValue().equals("FILE")) {
                            try {
                                dataMatcherMap.put(ruleMap, new SetMatcher(nNode2.getAttributes().getNamedItem("details").getNodeValue()));
                            } catch (IOException | URISyntaxException e) {
                                e.printStackTrace();
                            }
                        } else {
                            dataMatcherMap.put(ruleMap, new PatternMatcher(nNode2.getAttributes().getNamedItem("details").getNodeValue()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Could not parse data class library '{}'...", filePath);
            e.printStackTrace();
        }
    }

}
