/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Main class for Ripcurrent application. This application monitors for database changes using Debezium embedded engine connectors,
 * and will dynamically generated and run sortcl scripts to transport data to target tables, with any transformations
 * consistently applied based on rules mapped to data classes.
 *
 * Contributors:
 *     devonk
 */

import com.google.gson.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Main {
    final static String DATA_CLASS_LIBRARY_PROPERTY_NAME = "dataClassLibraryPath";
    final static String DATA_TARGET_PROCESS_TYPE_PROPERTY_NAME = "dataTargetProcessType";
    final static String DATA_TARGET_PROPERTY_NAME = "dataTarget";
    final static String DATA_TARGET_SCHEMA_PROPERTY_NAME = "dataTargetSchema";
    final static String DATA_TARGET_SEPARATOR_PROPERTY_NAME = "dataTargetSeparator";
    final static String RULES_LIBRARY_PROPERTY_NAME = "rulesLibraryPath";
    final static String STRUCTURE_CHANGE_LOG_PROPERTY_NAME = "schemaChangeEventLog";
    final static String TARGET_NAME_POSTFIX_PROPERTY_NAME = "targetNamePostfix";
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static AtomicReference<ConcurrentHashMap<String, SclScript>> scripts = new AtomicReference<>(); // Holds references to current SortCL jobs.

    JsonObject afterJsonPayload;
    ArrayList<String> columns = new ArrayList<>(); // A list of column names for the specific source table.
    DataClassRuleLibrary dataClassLibrary; // Ripcurrent will attempt to parse an existing IRI data class library when its path is specified as a Java property to the application.
    String dataTargetProcessType; // Process type for the data target.
    String dataTargetSchema; // Schema for the data target (if using ODBC).
    String dataTargetSeparator; // Separator to place in the SortCL script for the data target.
    JsonArray fieldsArray;
    FileOutputStream fileOutputStream;
    AtomicReference<Integer> i = new AtomicReference<>(); // A key to identify a specific SortCL job in the map of jobs.
    JsonObject jsonObject; // The Debezium change event is in JSON.
    String postfixTableName; // Target postfix string.
    Properties props; // Java configuration properties.
    RulesLibrary rulesLibrary; // Ripcurrent will attempt to parse an existing IRI rules library when its path is specified as a Java property to the application.
    String structureChangeEventLogPath;
    String DSN;

    private static void loadLog4jConfiguration() throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(System.getProperty("log4j.configurationFile")));
        LogManager.resetConfiguration();
        PropertyConfigurator.configure(properties);
    }

    private static String getSchema(Main m, JsonObject source) {
        switch (m.getProps().getProperty("connector.class")) {
            case "io.debezium.connector.mysql.MySqlConnector":
                return source.get("db").getAsString();
            case "io.debezium.connector.vitess.VitessConnector":
                return source.get("keyspace").getAsString();
            default:
                return source.get("schema").getAsString();
        }
    }

    // Main method; parse properties, data class library, rules library, and start Debezium engine.
    public static void main(String[] args) throws Exception {
        try {
            loadLog4jConfiguration();
        } catch (IOException e) {
            System.out.println("Error: Could not load log4j configuration.");
            System.exit(2);
        }
        String ripcurrentHome = null;
        try {
            ripcurrentHome = System.getProperty("APP_HOME");
            if (ripcurrentHome == null) {
                LOG.error("Could not detect property value for APP_HOME. Exiting...");
                System.exit(2);
            }
        } catch (NullPointerException | SecurityException e) {
            LOG.error("Could not detect property value for APP_HOME. Exiting...");
            System.exit(2);
        }
        LOG.info("Launching Debezium embedded engine");
        Properties props;
        props = new Properties(System.getProperties());
        Main m = new Main();
        Path ripcurrentConfigPath = java.nio.file.Paths.get(ripcurrentHome, "conf", "config.properties");
        try (InputStream input = new FileInputStream(ripcurrentConfigPath.toAbsolutePath().toString())) {
            // load a properties file
            props.load(input);
        } catch (IOException ex) {
            LOG.warn("Unable to load 'config.properties' from '{}'; Assuming all configuration properties have been set as system properties...", ripcurrentConfigPath);
        }
        m.setStructureChangeEventLogPath(props.getProperty(STRUCTURE_CHANGE_LOG_PROPERTY_NAME) == null ? "schema_change_events.log" : props.getProperty(STRUCTURE_CHANGE_LOG_PROPERTY_NAME));
        m.setFileOutputStream(new FileOutputStream(m.getStructureChangeEventLogPath(), true));
        String rulesLibraryPathString;
        String dataClassLibraryPathString;
        rulesLibraryPathString = props.getProperty(RULES_LIBRARY_PROPERTY_NAME);
        dataClassLibraryPathString = props.getProperty(DATA_CLASS_LIBRARY_PROPERTY_NAME);
        String dataTargetSeparator;
        dataTargetSeparator = props.getProperty(DATA_TARGET_SEPARATOR_PROPERTY_NAME);
        if (dataTargetSeparator != null && dataTargetSeparator.length() > 0) {
            m.setDataTargetSeparator(dataTargetSeparator);
        } else {
            m.setDataTargetSeparator("\\t");
        }
        String dataTargetSchema = props.getProperty(DATA_TARGET_SCHEMA_PROPERTY_NAME);
        if (dataTargetSchema != null && dataTargetSchema.length() > 0) {
            m.setDataTargetSchema(dataTargetSchema);
        }
        String targetNamePostfix = props.getProperty(TARGET_NAME_POSTFIX_PROPERTY_NAME);
        if (targetNamePostfix == null) {
            LOG.warn("{} property not set. Target table name will be the same as source table name.", TARGET_NAME_POSTFIX_PROPERTY_NAME);
            m.setPostfixTableName("");
        } else {
            m.setPostfixTableName(targetNamePostfix);
        }
        if (rulesLibraryPathString == null) {
            LOG.warn("{} property not set. Please set this property to the absolute path of an IRI rules library to apply rules.", RULES_LIBRARY_PROPERTY_NAME);
        }
        if (dataClassLibraryPathString == null) {
            LOG.warn("{} property not set. Please set this property to the absolute path of an IRI data class library to classify data.", DATA_CLASS_LIBRARY_PROPERTY_NAME);
        }
        // Set a few default properties - SortCL is expecting string representations of values.
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("binary.handling.mode", "base64");
        DataClassRuleLibrary dataClassLibrary = new DataClassRuleLibrary(props.getProperty(DATA_CLASS_LIBRARY_PROPERTY_NAME));
        m.setDataClassLibrary(dataClassLibrary);
        m.setProps(props);
        m.getI().set(0);
        String dataTargetProcessTypePropertyValue = m.getProps().getProperty(DATA_TARGET_PROCESS_TYPE_PROPERTY_NAME);
        if (dataTargetProcessTypePropertyValue == null) {
            m.setDataTargetProcessType("ODBC");
        } else {
            m.setDataTargetProcessType(dataTargetProcessTypePropertyValue);
        }
        m.setDSN(m.getProps().getProperty("DSN"));
        scripts.set(new ConcurrentHashMap<>());
        try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying(record -> {
                    if (record.value() != null) {
                        String scriptsKey = null;
                        try {
                            JsonObject jsonObject = JsonParser.parseString(record.value()).getAsJsonObject();
                            m.setJsonObject(jsonObject);
                            String operation = "";
                            if (jsonObject != null && jsonObject.get("payload") != null && jsonObject.get("payload").getAsJsonObject() != null && jsonObject.get("payload").getAsJsonObject().get("op") != null) {
                                operation = jsonObject.get("payload").getAsJsonObject().get("op").getAsString();
                            }
                            if (operation.equals("c") || operation.equals("u") && m.getDSN() != null || (operation.equals("d") && m.getDSN() != null)) { // Rows added or updated
                                JsonObject Jobject_;
                                if (operation.equals("d")) {
                                    Jobject_ = jsonObject.get("payload").getAsJsonObject().get("before").getAsJsonObject();
                                } else {
                                    Jobject_ = jsonObject.get("payload").getAsJsonObject().get("after").getAsJsonObject();
                                }
                                m.setAfterJsonPayload(Jobject_);
                                m.getColumns().addAll(Jobject_.keySet());
                                int count = 0;
                                boolean makeNewScript = Boolean.FALSE;
                                String keyField = null;
                                if (record.key() != null) {
                                    try {
                                        keyField = JsonParser.parseString(record.key()).getAsJsonObject().get("Schema").getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject().get("field").getAsString();
                                    } catch (JsonParseException | IllegalStateException | NullPointerException | IndexOutOfBoundsException e) {
                                        LOG.debug("Cannot parse primary key.");
                                    }
                                }
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
                                        if (!jj.getValue().isJsonNull()) {
                                            jj.setValue(new JsonPrimitive(DateTimeConversionUtil.integerToDate(jj.getValue().getAsInt())));
                                        }
                                    } else if (dateTimeIndices.contains(loopTrack)) {
                                        if (!jj.getValue().isJsonNull()) {
                                            jj.setValue(new JsonPrimitive(DateTimeConversionUtil.numberToDateTime(jj.getValue().getAsLong())));
                                        }
                                    } else if (timeIndices.contains(loopTrack)) {
                                        if (!jj.getValue().isJsonNull()) {
                                            jj.setValue(new JsonPrimitive(DateTimeConversionUtil.numberToTime(jj.getValue().getAsLong())));
                                        }
                                    }
                                    loopTrack++;
                                }
                                if (scripts.get() != null) {
                                    scriptsKey = String.valueOf(scripts.get().values().size() - 1);
                                } else {
                                    scriptsKey = "0";
                                }
                                if (scripts.get() != null && scripts.get().values().size() > 0) {
                                    for (SclScript script : scripts.get().values()) {

                                        if (!script.getOperation().equals(operation) || !script.getSourceTableIdentifier().equals(getSchema(m, jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject()) + "." + jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString()) || !script.getFields().stream()
                                                .map(SclField::getName)
                                                .collect(Collectors.toList()).equals(m.getColumns())) {
                                            count++;
                                            if (count == scripts.get().size()) { // If the table is new, make a new script.
                                                makeNewScript = true;
                                                scriptsKey = String.valueOf(count);
                                            }
                                        } else {
                                            scriptsKey = script.getKey();
                                            break;
                                        }
                                    }
                                } else {
                                    makeNewScript = true;
                                    scriptsKey = m.getI().get().toString();
                                }
                                if (makeNewScript) {
                                    makeANewScript(m, operation, keyField);
                                }
                                int ct = 0;
                                for (Map.Entry<String, JsonElement> jj : Jobject_.entrySet()) {
                                    ct++;
                                    String val;
                                    try {
                                        val = jj.getValue().getAsString();
                                    } catch (UnsupportedOperationException unsupportedOperationException) {
                                        val = "";
                                    }
                                    LOG.debug(val);
                                    try {
                                        scripts.get().get(scriptsKey).getStdin().write(val);
                                        if (ct < Jobject_.entrySet().size()) {
                                            scripts.get().get(scriptsKey).getStdin().write("\t");
                                        }
                                    } catch (IOException e) {
                                        LOG.error("Could not write output to target table '{}'. Aborting...", scripts.get().get(scriptsKey).getTargetTableIdentifier());
                                        terminateSortCLScript(scriptsKey, m);
                                    }
                                }

                                try {
                                    scripts.get().get(scriptsKey).getStdin().newLine();
                                    scripts.get().get(scriptsKey).getStdin().flush();
                                } catch (IOException e) { // Pipe is closed. This could happen if there was an error outputting to the target table.
                                    LOG.error("Could not flush output of replication job associated with table '{}'. Aborting...", scripts.get().get(scriptsKey).getSourceTableIdentifier());
                                    terminateSortCLScript(scriptsKey, m);
                                }
                                if (makeNewScript) {
                                    m.getI().set(m.getI().get() + 1);
                                }
                                m.getColumns().clear();
                            } else if (operation.equals("")) {
                                try {
                                    String database = getSchema(m, jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject());
                                    String table = jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString();
                                    String ddl = jsonObject.get("payload").getAsJsonObject().get("ddl").getAsString().replaceAll("\\R", " ");
                                    String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
                                    m.getFileOutputStream().write(String.format("%s: Database structure change event '%s' detected for table '%s.%s'.\n", timeStamp, ddl, database, table).getBytes(StandardCharsets.UTF_8));
                                } catch (Exception e) {
                                    try {
                                        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
                                        m.getFileOutputStream().write(String.format("%s: Database structure change event detected.\n", timeStamp).getBytes(StandardCharsets.UTF_8));

                                    } catch (IOException IoError) {
                                        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
                                        System.out.println(String.format("%s: WARNING: Unable to write to database change event log '%s'.", timeStamp, m.getStructureChangeEventLogPath()));
                                    }
                                }
                            }
                        } catch (NullPointerException npe) {
                            terminateSortCLScript(scriptsKey, m);
                        } catch (Exception unexpectedException) {
                            unexpectedException.printStackTrace();
                            LOG.error("Unexpected exception encountered: '{}'. Terminating...", unexpectedException.getMessage());
                            terminateSortCLScript(scriptsKey, m);
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
                    terminateSortCLScript("0", m);
                    closeSchemaChangeEventLog(m);
                    LOG.info("Engine terminated");
                } catch (IOException e) {
                    LOG.error("Unable to shutdown Debezium engine properly.", e);
                }
            }));
            // the submitted task keeps running, only no more new ones can be added
            executor.shutdown();
            awaitTermination(executor);
        }
    }

    // May allow for a more graceful termination.
    private static void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.debug("Debezium embedded engine running...");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // Applying rules to columns based on data classes.
    public static void classify(Set<Map.Entry<String, JsonElement>> values, DataClassRuleLibrary dataClassLibrary, ArrayList<SclField> fields) {
        int count = 0;
        for (Map.Entry<String, JsonElement> value : values) {
            for (Map.Entry<Map<String, Rule>, DataClassMatcher> entry : dataClassLibrary.dataMatcherMap.entrySet()) {
                if (!value.getValue().isJsonNull()) {
                    if (entry.getValue().getDataMatcher().isMatch(value.getValue().getAsString()) || entry.getValue().getNameMatcher().isMatch(fields.get(count).name)) {
                        fields.get(count).expressionApplied = true;
                        Rule rule = (Rule) entry.getKey().values().toArray()[0];
                        fields.get(count).expression = rule.getRule();
                        fields.get(count).ruleType = rule.getType();
                        break;
                    }
                }
            }
            count++;
        }
    }

    // A change event has been detected that requires a new script to be generated and executed.
    public static void makeANewScript(Main m, String operation, String keyField) {
        int loopTrack = 0;
        File tempFile = null;
        try {
            tempFile = File.createTempFile("sortcl", ".tmp");
        } catch (IOException e) {
            LOG.error("An error occurred when creating a temporary file.", e);
        }
        assert tempFile != null;
        tempFile.deleteOnExit();
        try { // Generating the SortCL script dynamically.
            FileWriter myWriter = new FileWriter(tempFile);
            String dataTarget = m.getProps().getProperty(DATA_TARGET_PROPERTY_NAME);
            String dataTargetProcessType = m.getProps().getProperty(DATA_TARGET_PROCESS_TYPE_PROPERTY_NAME);
            String sourceTable = m.getJsonObject().get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString();
            String sourceSchema = getSchema(m, m.getJsonObject().get("payload").getAsJsonObject().get("source").getAsJsonObject());
            String targetSchema = m.getDataTargetSchema();
            if (dataTarget != null) {
                try {
                    Path dataTargetPath = Paths.get(dataTarget);
                    String DSN = m.getDSN();
                    if (DSN != null) {
                        scripts.get().put(m.getI().get().toString(), new SclScript(sourceTable, sourceSchema, targetSchema, m.getColumns(), operation, dataTargetProcessType, dataTargetPath, m.getPostfixTableName(), DSN));
                    } else {
                        scripts.get().put(m.getI().get().toString(), new SclScript(sourceTable, sourceSchema, m.getColumns(), operation, dataTargetProcessType, dataTargetPath, m.getPostfixTableName()));
                    }
                } catch (InvalidPathException invalidPathException) {
                    LOG.error("Invalid target path for replication '{}'...", dataTarget);
                }
            } else {
                scripts.get().put(m.getI().get().toString(), new SclScript(sourceTable, sourceSchema, targetSchema, m.getDSN(), m.getColumns(), operation, m.getPostfixTableName()));
            }
            scripts.get().get(m.getI().toString()).setKey(m.getI().toString());
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
            myWriter.write(sortCLScript(scripts.get().get(m.getI().get().toString()), m, keyField));
            myWriter.close();
            LOG.info("New SortCL replication job started for table '{}'.", scripts.get().get(m.getI().get().toString()).getSourceTableIdentifier());
        } catch (IOException e) {
            LOG.error("An error occurred when writing a SortCL script to a temporary file.");
            closeSchemaChangeEventLog(m);
            System.exit(1);
        }

        try {
            scripts.get().get(m.getI().toString()).setProcess(new ProcessBuilder("sortcl", "/SPEC=" + tempFile.getAbsolutePath()).redirectErrorStream(true).start());
        } catch (IOException e) {
            LOG.error("An error occurred when starting sortcl process.");
            closeSchemaChangeEventLog(m);
            System.exit(1);
        }
    }

    // Convert actual tab value to \t
    public static String escapeTab(String separator) {
        return separator.replace("\t", "\\t");
    }

    // Generating a SortCL script dynamically based on info from Debezium change events and any default rules associated with a data class.
    public static String sortCLScript(SclScript script, Main m, String keyField) {
        StringBuilder sb = new StringBuilder();
        sb.append("/INFILE=stdin\n/PROCESS=CONCH\n");
        int count = 0;
        for (SclField field : script.getFields()) {
            count++;
            sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append("ASCII").append(", POSITION=").append(count).append(", SEPARATOR=\"\\t\"").append(")\n");
        }
        sb.append("/STREAM\n");
        if (script.getTarget() != null && script.getOperation().equals("c")) {
            sb.append("/OUTFILE=").append(script.getTarget()).append("\n").append("/PROCESS=").append(script.getTargetProcessType() != null ? script.getTargetProcessType() : "RECORD").append("\n");
            sb.append("/APPEND\n");
            count = 0;
            for (SclField field : script.getFields()) {
                count++;
                if (field.expressionApplied) {
                    if (field.getRuleType() != null && field.getRuleType().equalsIgnoreCase("set")) { // Assuming the set file ends with extension .set - maybe think of a better conditional test later.
                        sb.append("/FIELD=(ALTERED_").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\", ").append("SET=").append(field.getExpression());
                    } else {
                        sb.append("/FIELD=(ALTERED_").append(field.getName()).append("=").append(field.getExpression().replace("${FIELDNAME}", field.getName())).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", ").append("SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\"");
                    }
                } else {
                    sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ").append("SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\"");
                }
                if (field.getPrecision() != -1) {
                    sb.append(", PRECISION=").append(field.getPrecision());
                }
                sb.append(")\n");
            }
        }
        if (script.getDSN() != null) {
            sb.append("/OUTFILE=\"").append(script.getTargetTableIdentifier()).append(";DSN=").append(script.getDSN()).append(";\"\n");
            sb.append("/PROCESS=ODBC\n");
            if (script.getOperation().equals("u")) { // Assuming that the first column is a primary key - I don't see any information from Debezium about what columns are keys.
                sb.append("/UPDATE=(");
                if (keyField != null) {
                    sb.append(keyField);
                } else {
                    sb.append(script.getFields().get(0).getName());
                }
                sb.append(")\n");
            } else if (script.getOperation().equals("d")) {
                sb.append("/DELETE=(");
                if (keyField != null) {
                    sb.append(keyField);
                } else {
                    sb.append(script.getFields().get(0).getName());
                }
                sb.append(")\n");
            } else {
                sb.append("/APPEND\n");
            }
            count = 0;
            for (SclField field : script.getFields()) {
                count++;
                if (field.expressionApplied) {
                    if (field.getRuleType() != null && field.getRuleType().equalsIgnoreCase("set")) {
                        sb.append("/FIELD=(ALTERED_").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\", ").append("SET=").append(field.getExpression());
                    } else {
                        sb.append("/FIELD=(ALTERED_").append(field.getName()).append("=").append(field.getExpression().replace("${FIELDNAME}", field.getName())).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", ").append("SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\"");
                    }
                } else {
                    sb.append("/FIELD=(").append(field.getName()).append(", TYPE=").append(field.getDataType()).append(", POSITION=").append(count).append(", ").append("SEPARATOR=\"").append(escapeTab(m.getDataTargetSeparator())).append("\"");
                }
                if (field.getPrecision() != -1) {
                    sb.append(", PRECISION=").append(field.getPrecision());
                }
                sb.append(")\n");
            }
        }
        if (script.getDSN() == null && script.getTarget() == null) {
            sb.append("/OUTFILE=stdout\n");
        }
        return sb.toString();
    }

    public static void closeSchemaChangeEventLog(Main m) {
        try {
            m.getFileOutputStream().close();
        } catch (IOException e) {
            LOG.warn("Could not close schema change event log file '{}'.", m.getStructureChangeEventLogPath());
        }
    }

    // An error happened in sortcl execution. This prints the error output to the log and terminates the application to give the user a chance to review and correct the error.
    // When the application is started again, Debezium will pick up at the same spot in the database log.
    public static void terminateSortCLScript(String scriptsKey, Main m) {
        StringBuilder errorMessage = new StringBuilder();
        String line;
        try {
            while ((line = scripts.get().get(scriptsKey).getStdout().readLine()) != null) {
                errorMessage.append(line);
                errorMessage.append("\n");
            }
        } catch (IOException | NullPointerException e) {
            LOG.warn("Could not retrieve SortCL output.");
        }
        if (scripts.get().get(scriptsKey) != null) {
            LOG.error("SortCL replication job for table '{}' encountered an error:\n{}\nThe job is being terminated.\nCheck the .cserrlog for possible details on the cause of the error.", scripts.get().get(scriptsKey).getSourceTableIdentifier(), errorMessage);
            for (Map.Entry<String, SclScript> script : scripts.get().entrySet()) {
                try {
                    scripts.get().get(script.getKey()).getStdin().close();
                } catch (IOException e) {
                    LOG.warn("Failed to close the stdin to local CoSort job: {}.", e.getMessage());
                }
                try {
                    scripts.get().get(script.getKey()).getStderr().close();
                } catch (IOException e) {
                    LOG.warn("Failed to close the stderr to local CoSort job: {}.", e.getMessage());
                }
                try {
                    scripts.get().get(script.getKey()).getStdout().close();
                } catch (IOException e) {
                    LOG.warn("Failed to close the stdout to local CoSort job: {}.", e.getMessage());
                }
                scripts.get().get(script.getKey()).getProcess().destroy();
                scripts.get().remove(scriptsKey);
            }
        }
        closeSchemaChangeEventLog(m);
        System.exit(1);
    }

    public JsonObject getAfterJsonPayload() {
        return afterJsonPayload;
    }

    public void setAfterJsonPayload(JsonObject afterJsonPayload) {
        this.afterJsonPayload = afterJsonPayload;
    }

    public ArrayList<String> getColumns() {
        return columns;
    }

    public void setColumns(ArrayList<String> columns) {
        this.columns = columns;
    }

    public DataClassRuleLibrary getDataClassLibrary() {
        return dataClassLibrary;
    }

    public void setDataClassLibrary(DataClassRuleLibrary dataClassLibrary) {
        this.dataClassLibrary = dataClassLibrary;
    }

    public String getDataTargetProcessType() {
        return dataTargetProcessType;
    }

    public void setDataTargetProcessType(String dataTargetProcessType) {
        this.dataTargetProcessType = dataTargetProcessType;
    }

    public String getDataTargetSchema() {
        return dataTargetSchema;
    }

    public void setDataTargetSchema(String dataTargetSchema) {
        this.dataTargetSchema = dataTargetSchema;
    }

    public String getDataTargetSeparator() {
        return dataTargetSeparator;
    }

    public void setDataTargetSeparator(String dataTargetSeparator) {
        this.dataTargetSeparator = dataTargetSeparator;
    }

    public JsonArray getFieldsArray() {
        return fieldsArray;
    }

    public void setFieldsArray(JsonArray fieldsArray) {
        this.fieldsArray = fieldsArray;
    }

    public AtomicReference<Integer> getI() {
        return i;
    }

    public void setI(AtomicReference<Integer> i) {
        this.i = i;
    }

    public JsonObject getJsonObject() {
        return jsonObject;
    }

    public void setJsonObject(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public String getPostfixTableName() {
        return postfixTableName;
    }

    public void setPostfixTableName(String postfixTableName) {
        this.postfixTableName = postfixTableName;
    }

    public Properties getProps() {
        return props;
    }

    public void setProps(Properties props) {
        this.props = props;
    }

    public RulesLibrary getRulesLibrary() {
        return rulesLibrary;
    }

    public void setRulesLibrary(RulesLibrary rulesLibrary) {
        this.rulesLibrary = rulesLibrary;
    }

    public FileOutputStream getFileOutputStream() {
        return fileOutputStream;
    }

    public void setFileOutputStream(FileOutputStream fileOutputStream) {
        this.fileOutputStream = fileOutputStream;
    }

    public String getStructureChangeEventLogPath() {
        return structureChangeEventLogPath;
    }

    public void setStructureChangeEventLogPath(String structureChangeEventLogPath) {
        this.structureChangeEventLogPath = structureChangeEventLogPath;
    }

    public String getDSN() {
        return DSN;
    }

    public void setDSN(String DSN) {
        this.DSN = DSN;
    }
}