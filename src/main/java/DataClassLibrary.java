import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class DataClassLibrary {
    Map<String, DataMatcher> dataMatcherMap = new HashMap<>();

    DataClassLibrary(String filePath) {
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
                        defaultRule = eElement.getElementsByTagName("defaultRule").item(0).getNodeValue().split("#");
                    } catch (NullPointerException e) {
                        defaultRule = new String[]{"#", "enc_fp_aes256_alphanum"};
                    }
                    NodeList matchersList = eElement.getElementsByTagName("matchers");
                    for (int temp2 = 0; temp2 < matchersList.getLength(); temp2++) {
                        Node nNode2 = matchersList.item(temp2);
                        if (nNode2.getAttributes().getNamedItem("type") != null && nNode2.getAttributes().getNamedItem("type").getNodeValue().equals("FILE")) {
                            dataMatcherMap.put(defaultRule[1], new SetMatcher(nNode2.getAttributes().getNamedItem("details").getNodeValue()));
                        } else {
                            dataMatcherMap.put(defaultRule[1], new PatternMatcher(nNode2.getAttributes().getNamedItem("details").getNodeValue()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
