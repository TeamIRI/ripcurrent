import java.util.HashMap;
import java.util.Map;

public class RulesLibrary {
    Map<String, String> rules = new HashMap<>();

    // name, rule
    RulesLibrary() {
       /* try {
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
                                    rules.put(((Element) nNode).getAttribute("name"), nNode2.getAttributes().getNamedItem("value").getNodeValue().replace("(${FIELDNAME})", ""));
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
            e.printStackTrace();
        }
        */

    }

    public Map<String, String> getRules() {
        return rules;
    }

    public void setRules(Map<String, String> rules) {
        this.rules = rules;
    }
}
