/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Read from an existing IRI data class library file.
 *
 * Contributors:
 *     devonk
 */

import org.apache.xerces.dom.DeferredElementImpl;
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

public class DataClassRuleLibrary {
    private static final Logger LOG = LoggerFactory.getLogger(DataClassRuleLibrary.class);
    Map<Map<String, Rule>, DataClassMatcher> dataMatcherMap = new HashMap<>();

    Map<String, Rule> rules = new HashMap<>();

    DataClassRuleLibrary(String filePath) {
        try {
            File dataClassLibraryFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(dataClassLibraryFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("dcv2s");
            NodeList rulesList = doc.getElementsByTagName("rules");
            for (int temp = 0; temp < rulesList.getLength(); temp++) {
                Node nNode = rulesList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    NodeList matchersList = eElement.getElementsByTagName("properties");
                    for (int temp2 = 0; temp2 < matchersList.getLength(); temp2++) {
                        Node nNode2 = matchersList.item(temp2);
                        switch (nNode2.getAttributes().getNamedItem("dataRulePropertyType").getNodeValue()) {
                            case "EXPRESSION":
                                rules.put(((Element) nNode).getAttribute("name"), new Rule("Expression", nNode2.getAttributes().getNamedItem("value").getNodeValue()));
                                break;
                            case "SET":
                                rules.put(((Element) nNode).getAttribute("name"), new Rule("Set", nNode2.getAttributes().getNamedItem("value").getNodeValue().replace("&quot;", "").replace(" SELECT=ANY", "")));
                                break;
                            default:
                        }
                    }
                }
            }
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    String defaultRule = "";
                    String ruleExpression = "";
                    try {
                        int length = ((DeferredElementImpl) eElement.getChildNodes()).getLength();
                        for (int aa = 0; aa < length; aa++) {
                            String nodeName = ((DeferredElementImpl) eElement.getChildNodes()).item(aa).getNodeName();
                            if (!nodeName.equals("defaultRule")) {
                                continue;
                            }
                            defaultRule = eElement.getChildNodes().item(aa).getAttributes().getNamedItem("name").getNodeValue();
                            if (rules.get(defaultRule) != null) {
                                ruleExpression = rules.get(defaultRule).getRule();
                            } else { // Default
                                continue;
                            }
                        }
                    } catch (NullPointerException e) {
                        continue;
                    }

                    NodeList matchersList = eElement.getElementsByTagName("matchers");
                    for (int temp2 = 0; temp2 < matchersList.getLength(); temp2++) {
                        Node nNode2 = matchersList.item(temp2);
                        HashMap<String, Rule> ruleMap = new HashMap<>();
                        ruleMap.put(((Element) nNode).getAttribute("name"), new Rule(rules.get(defaultRule).getType(), ruleExpression));
                        if (nNode2.getAttributes().getNamedItem("type") != null && nNode2.getAttributes().getNamedItem("type").getNodeValue().equals("FILE")) {
                            try {
                                dataMatcherMap.put(ruleMap, new DataClassMatcher(new NameMatcher(""), new SetMatcher(nNode2.getAttributes().getNamedItem("path").getNodeValue())));
                            } catch (IOException | URISyntaxException e) {
                                LOG.warn("Set file '{}' does not exist...", nNode2.getAttributes().getNamedItem("details").getNodeValue());
                            }
                        } else {
                            if (nNode2.getAttributes().getNamedItem("pattern") != null && nNode2.getAttributes().getNamedItem("usedFor") != null && nNode2.getAttributes().getNamedItem("usedFor").getNodeValue().equals("LOCATION")) {
                                dataMatcherMap.put(ruleMap, new DataClassMatcher(new NameMatcher(nNode2.getAttributes().getNamedItem("pattern").getNodeValue()), new PatternMatcher("")));
                            }
                            else if (nNode2.getAttributes().getNamedItem("pattern") != null && (nNode2.getAttributes().getNamedItem("usedFor") == null || !nNode2.getAttributes().getNamedItem("usedFor").getNodeValue().equals("LOCATION"))) {
                                dataMatcherMap.put(ruleMap, new DataClassMatcher(new NameMatcher(""), new PatternMatcher(nNode2.getAttributes().getNamedItem("pattern").getNodeValue())));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not parse data class rule library '{}'...", filePath, e);
        }
    }

}
