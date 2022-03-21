import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Main {
    static AtomicReference<HashMap<String, SclScript>> scripts = new AtomicReference<>();
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.info("Waiting another 10 seconds for the embedded engine to complete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static String sortCLScript(SclScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("/INFILE=stdin\n/PROCESS=CONCH\n");
        int count = 0;
        for (SclField field : script.getFields()) {
            count++;
            sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", SEPARATOR=\"\\t\")\n");
        }
        sb.append("/STREAM\n");
        sb.append("/OUTFILE=\"").append(script.getTable()).append(";DSN=").append(script.getDSN()).append(";\"\n");
        sb.append("/PROCESS=ODBC\n");
        // sb.append("/UPDATE=(" + script.getFields().get(0) + ")\n");
        count = 0;
        for (SclField field : script.getFields()) {
            count++;
            //    if (count > 1) {
            if (field.expressionApplied) {
                if (field.getExpression().contains(".set")) { // Assuming the set file ends with extension .set - maybe think of a better conditional test later.
                    sb.append("/FIELD=(ALTERED_").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"\\t\", ").append("SET=").append(field.getExpression()).append(")\n");
                } else {
                    sb.append("/FIELD=(ALTERED_").append(field.getName()).append("=").append(field.getExpression()).append("(").append(field.getName()).append("), TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"\\t\")\n");
                }

            } else {
                sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", SEPARATOR=\"\\t\")\n");
            }
        }
        return sb.toString();
    }

    public static void classify(Set<Map.Entry<String, JsonElement>> values, DataClassLibrary dataClassLibrary, ArrayList<SclField> fields) {
        int count = 0;
        for (Map.Entry<String, JsonElement> value : values) {
            for (Map.Entry<Map<String, String>, DataMatcher> entry : dataClassLibrary.dataMatcherMap.entrySet()) {
                if (entry.getValue().isMatch(value.getValue().getAsString())) {
                    fields.get(count).expressionApplied = true;
                    fields.get(count).expression = (String) entry.getKey().values().toArray()[0];
                    break;
                }
            }
            count++;
        }
    }


    public static void main(String[] args) throws Exception {
        ArrayList<String> columns = new ArrayList<>();
        LOG.info("Launching Debezium embedded engine");
        Properties props;
        try (InputStream input = new FileInputStream("config.properties")) {

            props = new Properties();

            // load a properties file
            props.load(input);

        } catch (IOException ex) {
            LOG.error("Unable to load 'config.properties', needed for configuration and database connection details. Exiting...");
            return;
        }
        RulesLibrary rulesLibrary = new RulesLibrary("iriLibrary.rules");
        DataClassLibrary dataClassLibrary = new DataClassLibrary("iriLibrary.dataclass", rulesLibrary.getRules());
        AtomicReference<Integer> i = new AtomicReference<>();
        i.set(0);

        scripts.set(new HashMap<>());
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    if (record.value() != null) {
                        try {
                            JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
                            String operation = "";
                            if (jsonObject != null && jsonObject.get("payload") != null && jsonObject.get("payload").getAsJsonObject() != null && jsonObject.get("payload").getAsJsonObject().get("op") != null) {
                                operation = jsonObject.get("payload").getAsJsonObject().get("op").getAsString();
                            }
                            if (operation.equals("c")) { // Rows added
                                JsonObject Jobject_ = jsonObject.get("payload").getAsJsonObject().get("after").getAsJsonObject();
                                columns.addAll(Jobject_.keySet());
                                int count = 0;
                                boolean makeNewScript = Boolean.FALSE;
                                if (scripts.get() != null && scripts.get().values().size() > 0) {
                                    for (SclScript script : scripts.get().values()) {

                                        if (!script.getTable().equals(jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString() + "_masked") || !script.getFields().stream()
                                                .map(SclField::getName)
                                                .collect(Collectors.toList()).equals(columns)) {
                                            count++;
                                            if (count == scripts.get().size()) { // If the table is new, make a new script.
                                                makeNewScript = true;
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                                } else {
                                    makeNewScript = true;
                                }
                                if (makeNewScript) {
                                    File tempFile = null;
                                    try {
                                        tempFile = File.createTempFile("sortcl", ".tmp");
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                    assert tempFile != null;
                                    tempFile.deleteOnExit();
                                    try { // Generating the SortCL script dynamically.
                                        FileWriter myWriter = new FileWriter(tempFile);
                                        scripts.get().put(i.get().toString(), new SclScript(jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString(), props.getProperty("DSN"), columns));
                                        JsonArray fieldsArray = jsonObject.get("schema").getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject().get("fields").getAsJsonArray();
                                        int loopTrack = 0;
                                        for (JsonElement object : fieldsArray) {
                                            String type = object.getAsJsonObject().get("type").getAsString();
                                            JsonElement name = object.getAsJsonObject().get("name");
                                            if (type == null) {
                                                loopTrack++;
                                                continue;
                                            }
                                            switch (type) {
                                                case "int32":
                                                    if (name == null) {
                                                        scripts.get().get(i.get().toString()).getFields().get(loopTrack).setDataType("WHOLE_NUMBER");
                                                    } else {
                                                        scripts.get().get(i.get().toString()).getFields().get(loopTrack).setDataType("ISO_DATE");
                                                    }
                                                    break;
                                                default:

                                            }
                                            loopTrack++;
                                        }
                                        classify(Jobject_.entrySet(), dataClassLibrary, scripts.get().get(i.get().toString()).getFields());
                                        myWriter.write(sortCLScript(scripts.get().get(i.get().toString())));
                                        myWriter.close();
                                        LOG.info("Successfully wrote SortCL script to the temporary file.");
                                    } catch (IOException e) {
                                        LOG.error("An error occurred when writing SortCL script to a temporary file.");
                                        e.printStackTrace();
                                    }

                                    try {
                                        scripts.get().get(i.toString()).setProcess(new ProcessBuilder("sortcl", "/SPEC=" + tempFile.getAbsolutePath()).redirectErrorStream(true).start());
                                    } catch (IOException e) {
                                        LOG.error("An error occurred when starting sortcl process.");
                                        e.printStackTrace();
                                    }
                                }


                                int ct = 0;
                                for (Map.Entry<String, JsonElement> jj : Jobject_.entrySet()) {
                                    ct++;
                                    LOG.debug(String.valueOf(jj.getValue()));
                                    try {
                                        scripts.get().get(Integer.toString(count))
                                                .getStdin().write(jj.getValue().getAsString());
                                        if (ct < Jobject_.entrySet().size()) {
                                            scripts.get().get(Integer.toString(count)).getStdin().write("\t");
                                        }
                                    } catch (IOException e) {
                                        LOG.error("Attempted to write to closed standard input of an existing sortcl process.");
                                        e.printStackTrace();
                                    }

                                }

                                try {
                                    scripts.get().get(Integer.toString(count)).getStdin().newLine();
                                    scripts.get().get(Integer.toString(count)).getStdin().flush();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                                if (makeNewScript) {
                                    i.set(i.get() + 1);
                                }
                                columns.clear();
                            }
                        } catch (NullPointerException ee) {
                            ee.printStackTrace();
                        }
                    }
                })
                .build()) {
            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("Requesting embedded engine to shut down");
                try {
                    engine.close();
                } catch (IOException e) {
                    LOG.error("Unable to shutdown Debezium engine properly.");
                    e.printStackTrace();
                }
            }));
            // the submitted task keeps running, only no more new ones can be added
            executor.shutdown();
            awaitTermination(executor);
            LOG.info("Engine terminated");
        }
    }
}

