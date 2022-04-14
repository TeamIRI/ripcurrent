/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Read from an IRI rules library file.
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
import java.util.HashMap;
import java.util.Map;

public class RulesLibrary {
    Map<String, String> rules = new HashMap<>();
    private static final Logger LOG = LoggerFactory.getLogger(RulesLibrary.class);
    // name, rule
    RulesLibrary(String filePath) {
        try {
            File dataClassLibraryFile = new File(filePath);
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(dataClassLibraryFile);
            doc.getDocumentElement().normalize();
            NodeList nList = doc.getElementsByTagName("rules");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    NodeList matchersList = eElement.getElementsByTagName("properties");
                    for (int temp2 = 0; temp2 < matchersList.getLength(); temp2++) {
                        Node nNode2 = matchersList.item(temp2);
                        switch (nNode2.getAttributes().getNamedItem("fieldRulePropertyType").getNodeValue()) {
                            case "EXPRESSION":
                                rules.put(((Element) nNode).getAttribute("name"), nNode2.getAttributes().getNamedItem("value").getNodeValue());
                                break;
                            case "SET":
                                rules.put(((Element) nNode).getAttribute("name"), nNode2.getAttributes().getNamedItem("value").getNodeValue().replace("&quot;", "").replace(" SELECT=ANY", ""));
                                break;
                            default:
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Could not parse rules library '{}'...", filePath);
            e.printStackTrace();
        }


    }

    public Map<String, String> getRules() {
        return rules;
    }

    public void setRules(Map<String, String> rules) {
        this.rules = rules;
    }
}
