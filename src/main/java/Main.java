/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Main class for Riptide application. This application monitors for database changes using Debezium embedded engine connectors,
 *  and will dynamically generated and run sortcl scripts to transport data to target tables, with any transformations
 *  consistently applied based on rules mapped to data classes.
 *
 * Contributors:
 *     devonk
 */
import com.google.gson.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Main {
    final static String DATA_CLASS_LIBRARY_PROPERTY_NAME = "dataClassLibraryPath";
    final static String RULES_LIBRARY_PROPERTY_NAME = "rulesLibraryPath";
    final static String TARGET_NAME_POSTFIX_PROPERTY_NAME = "targetNamePostfix";
    RulesLibrary rulesLibrary;
    DataClassLibrary dataClassLibrary;
    AtomicReference<Integer> i = new AtomicReference<>();
    JsonObject jsonObject;
    JsonObject afterJsonPayload;
    JsonArray fieldsArray;
    Properties props;
    ArrayList<String> columns = new ArrayList<>();

    public static void makeANewScript(Main m, String operation) {
        int loopTrack = 0;
        File tempFile = null;
        try {
            tempFile = File.createTempFile("sortcl", ".tmp");
        } catch (IOException e) {
            LOG.error("An error occurred when creating a temporary file.");
            e.printStackTrace();
        }
        assert tempFile != null;
        tempFile.deleteOnExit();
        try { // Generating the SortCL script dynamically.
            FileWriter myWriter = new FileWriter(tempFile);
            scripts.get().put(m.getI().get().toString(), new SclScript(m.getJsonObject().get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString(), m.getProps().getProperty("DSN"), m.getColumns(), operation));
            for (JsonElement object : m.getFieldsArray()) {
                String type = object.getAsJsonObject().get("type").getAsString();
                JsonElement name = object.getAsJsonObject().get("name");
                if (type == null) {
                    loopTrack++;
                    continue;
                }
                switch (type) {
                    case "int32":
                        if (name == null) {
                            scripts.get().get(m.getI().get().toString()).getFields().get(loopTrack).setDataType("NUMERIC");
                            scripts.get().get(m.getI().get().toString()).getFields().get(loopTrack).setPrecision(0);
                        } else {
                            scripts.get().get(m.getI().get().toString()).getFields().get(loopTrack).setDataType("ISO_DATE");
                        }
                        break;
                    default:

                }
                loopTrack++;
            }
            classify(m.getAfterJsonPayload().entrySet(), m.getDataClassLibrary(), scripts.get().get(m.getI().get().toString()).getFields());
            myWriter.write(sortCLScript(scripts.get().get(m.getI().get().toString())));
            myWriter.close();
            LOG.info("Successfully wrote SortCL script to the temporary file.");
        } catch (IOException e) {
            LOG.error("An error occurred when writing SortCL script to a temporary file.");
            e.printStackTrace();
        }

        try {
            scripts.get().get(m.getI().toString()).setProcess(new ProcessBuilder("sortcl", "/SPEC=" + tempFile.getAbsolutePath()).redirectErrorStream(true).start());
        } catch (IOException e) {
            LOG.error("An error occurred when starting sortcl process.");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        String riptideHome;
        try {
            riptideHome = System.getenv("RIPTIDE_HOME");
            if (riptideHome == null) {
                LOG.error("Could not detect environment variable value for RIPTIDE_HOME. Please set this environment variable to" +
                        " the root of the Riptide distribution folder.");
                return;
            }
        } catch (NullPointerException | SecurityException e) {
            LOG.error("Could not detect environment variable value for RIPTIDE_HOME. Please set this environment variable to" +
                    " the root of the Riptide distribution folder.");
            return;
        }
        LOG.info("Launching Debezium embedded engine");
        Properties props;
        Path riptideConfigPath = java.nio.file.Paths.get(riptideHome, "conf", "config.properties");
        try (InputStream input = new FileInputStream(riptideConfigPath.toAbsolutePath().toString())) {

            props = new Properties();

            // load a properties file
            props.load(input);

        } catch (IOException ex) {
            LOG.error("Unable to load 'config.properties' from '{}'; needed for configuration and database connection details. Exiting...", riptideConfigPath);
            return;
        }
        String rulesLibraryPathString;
        String dataClassLibraryPathString;
        rulesLibraryPathString = props.getProperty(RULES_LIBRARY_PROPERTY_NAME);
        dataClassLibraryPathString = props.getProperty(DATA_CLASS_LIBRARY_PROPERTY_NAME);
        if (rulesLibraryPathString == null) {
            LOG.warn("{} property not set. Please set this property to the absolute path of an IRI rules library.", RULES_LIBRARY_PROPERTY_NAME);
        }
        if (dataClassLibraryPathString == null) {
            LOG.warn("{} property not set. Please set this property to the absolute path of an IRI data class library.", DATA_CLASS_LIBRARY_PROPERTY_NAME);
        }

        RulesLibrary rulesLibrary = new RulesLibrary(props.getProperty(RULES_LIBRARY_PROPERTY_NAME));
        DataClassLibrary dataClassLibrary = new DataClassLibrary(props.getProperty(DATA_CLASS_LIBRARY_PROPERTY_NAME), rulesLibrary.getRules());
        Main m = new Main();
        m.setDataClassLibrary(dataClassLibrary);
        m.setRulesLibrary(rulesLibrary);
        m.setProps(props);
        m.getI().set(0);

        scripts.set(new HashMap<>());
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    if (record.value() != null) {
                        try {
                            JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
                            m.setJsonObject(jsonObject);
                            String operation = "";
                            if (jsonObject != null && jsonObject.get("payload") != null && jsonObject.get("payload").getAsJsonObject() != null && jsonObject.get("payload").getAsJsonObject().get("op") != null) {
                                operation = jsonObject.get("payload").getAsJsonObject().get("op").getAsString();
                            }
                            if (operation.equals("c") || operation.equals("u")) { // Rows added or updated
                                JsonObject Jobject_ = jsonObject.get("payload").getAsJsonObject().get("after").getAsJsonObject();
                                m.setAfterJsonPayload(Jobject_);
                                m.getColumns().addAll(Jobject_.keySet());
                                int count = 0;
                                boolean makeNewScript = Boolean.FALSE;
                                JsonArray fieldsArray = jsonObject.get("schema").getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject().get("fields").getAsJsonArray();
                                m.setFieldsArray(fieldsArray);
                                int loopTrack = 0;
                                List<Integer> dateIndices = new ArrayList<>();
                                List<Integer> dateTimeIndices = new ArrayList<>();
                                List<Integer> timeIndices = new ArrayList<>();
                                for (JsonElement object : fieldsArray) {
                                    String type = object.getAsJsonObject().get("type").getAsString();
                                    JsonElement name = object.getAsJsonObject().get("name");
                                    if (type == null) {
                                        loopTrack++;
                                        continue;
                                    }
                                    switch (type) {
                                        case "int32":
                                            if (name != null && name.getAsString().equals("io.debezium.time.Date")) { // This is the name for a date, at least with the MySQL connector. Actual Integers seem to have a null name.
                                                dateIndices.add(loopTrack);
                                            }
                                            break;
                                        case "int64":
                                            if (name != null && name.getAsString().equals("io.debezium.time.MicroTime")) { // This is the name for a time, at least with the MySQL connector. Actual Integers seem to have a null name.
                                                timeIndices.add(loopTrack);
                                            } else if (name != null && name.getAsString().equals("io.debezium.time.Timestamp")) { // This is the name for a timestamp, at least with the MySQL connector. Actual Integers seem to have a null name.
                                                dateTimeIndices.add(loopTrack);
                                            }
                                            break;
                                        default:

                                    }
                                    loopTrack++;
                                }
                                loopTrack = 0;
                                // Dates are coming in through the Debezium connector as numeric values, this is looking for them and converting them to their date representation.
                                for (Map.Entry<String, JsonElement> jj : Jobject_.entrySet()) {
                                    if (dateIndices.contains(loopTrack)) {
                                        jj.setValue(new JsonPrimitive(DateTimeConversionUtil.integerToDate(jj.getValue().getAsInt())));
                                    } else if (dateTimeIndices.contains(loopTrack)) {
                                        jj.setValue(new JsonPrimitive(DateTimeConversionUtil.numberToDateTime(jj.getValue().getAsLong())));
                                    } else if (timeIndices.contains(loopTrack)) {
                                        jj.setValue(new JsonPrimitive(DateTimeConversionUtil.numberToTime(jj.getValue().getAsLong())));
                                    }
                                    loopTrack++;
                                }

                                if (scripts.get() != null && scripts.get().values().size() > 0) {
                                    for (SclScript script : scripts.get().values()) {

                                        if (!script.getOperation().equals(operation) || !script.getTable().equals(jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString() + props.getProperty(TARGET_NAME_POSTFIX_PROPERTY_NAME)) || !script.getFields().stream()
                                                .map(SclField::getName)
                                                .collect(Collectors.toList()).equals(m.getColumns())) {
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
                                    makeANewScript(m, operation);
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
                                        // This could happen if there was an error outputting to the target table.
                                        scripts.get().remove(Integer.toString(count));
                                        makeANewScript(m, operation);
                                        makeNewScript = true;
                                        count = 0;
                                        if (scripts.get() != null && scripts.get().values().size() > 0) {
                                            for (SclScript script : scripts.get().values()) {

                                                if (!script.getTable().equals(jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString() + props.getProperty(TARGET_NAME_POSTFIX_PROPERTY_NAME)) || !script.getFields().stream()
                                                        .map(SclField::getName)
                                                        .collect(Collectors.toList()).equals(m.getColumns())) {
                                                    count++;
                                                } else {
                                                    break;
                                                }
                                            }
                                        }
                                        try {
                                            scripts.get().get(Integer.toString(count))
                                                    .getStdin().write(jj.getValue().getAsString());
                                            if (ct < Jobject_.entrySet().size()) {
                                                scripts.get().get(Integer.toString(count)).getStdin().write("\t");
                                            }
                                        } catch (IOException e2) {
                                            LOG.error("Could not write output to target table {}.", scripts.get().get(Integer.toString(count)).getTable());
                                            break;
                                        }
                                    }
                                }

                                try {
                                    scripts.get().get(Integer.toString(count)).getStdin().newLine();
                                    scripts.get().get(Integer.toString(count)).getStdin().flush();
                                } catch (IOException e) { // Pipe is closed; remove the broken script from script map.
                                    // This could happen if there was an error outputting to the target table.
                                    LOG.error("Could not flush output of script associated with table {}. Removing script...", scripts.get().get(Integer.toString(count)).getTable());
                                    scripts.get().remove(Integer.toString(count));
                                }
                                if (makeNewScript) {
                                    m.getI().set(m.getI().get() + 1);
                                }
                                m.getColumns().clear();
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

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public JsonArray getFieldsArray() {
        return fieldsArray;
    }

    public void setFieldsArray(JsonArray fieldsArray) {
        this.fieldsArray = fieldsArray;
    }

    public JsonObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public JsonObject getAfterJsonPayload() {
        return afterJsonPayload;
    }

    public void setAfterJsonPayload(JsonObject afterJsonPayload) {
        this.afterJsonPayload = afterJsonPayload;
    }

    public RulesLibrary getRulesLibrary() {
        return rulesLibrary;
    }

    public void setRulesLibrary(RulesLibrary rulesLibrary) {
        this.rulesLibrary = rulesLibrary;
    }

    public DataClassLibrary getDataClassLibrary() {
        return dataClassLibrary;
    }

    public void setDataClassLibrary(DataClassLibrary dataClassLibrary) {
        this.dataClassLibrary = dataClassLibrary;
    }

    static AtomicReference<HashMap<String, SclScript>> scripts = new AtomicReference<>();
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.debug("Debezium embedded engine running...");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public AtomicReference<Integer> getI() {
        return i;
    }

    public static String sortCLScript(SclScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("/INFILE=stdin\n/PROCESS=CONCH\n");
        int count = 0;
        for (SclField field : script.getFields()) {
            count++;
            sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append("ASCII").append(", POSITION=").append(count).append(", SEPARATOR=\"\\t\"").append(")\n");
        }
        sb.append("/STREAM\n");
        sb.append("/OUTFILE=\"").append(script.getTable()).append(";DSN=").append(script.getDSN()).append(";\"\n");
        sb.append("/PROCESS=ODBC\n");
        if (script.getOperation().equals("u")) { // Assuming that the first column is a primary key - I don't see any information from Debezium about what columns are keys.
            sb.append("/UPDATE=(");
            sb.append(script.getFields().get(0).getName());
            sb.append(")\n");
        }
        count = 0;
        for (SclField field : script.getFields()) {
            count++;
            if (field.expressionApplied) {
                if (field.getExpression().contains(".set")) { // Assuming the set file ends with extension .set - maybe think of a better conditional test later.
                    sb.append("/FIELD=(ALTERED_").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"\\t\", ").append("SET=").append(field.getExpression());
                } else {
                    sb.append("/FIELD=(ALTERED_").append(field.getName()).append("=").append(field.getExpression().replace("${FIELDNAME}", field.getName())).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"\\t\"");
                }
            } else {
                sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", SEPARATOR=\"\\t\"");
            }
            if (field.getPrecision() != -1) {
                sb.append(", PRECISION=").append(field.getPrecision());
            }
            sb.append(")\n");
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

    public void setI(AtomicReference<Integer> i) {
        this.i = i;
    }
}

