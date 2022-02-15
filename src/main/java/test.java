import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class test {
    static AtomicReference<HashMap<String, SclScript>> scripts = new AtomicReference<>();
    public static String sortCLScript(SclScript script) {
        StringBuilder sb = new StringBuilder();
        sb.append("/INFILE=stdin\n/PROCESS=CONCH\n");
        int count = 0;
        for (SclField field : script.getFields()) {
            count++;
            sb.append("/FIELD=(").append(field.getName()).append(", TYPE=ASCII, POSITION=").append(count).append(", SEPARATOR=\"\\t\")\n");
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
                sb.append("/FIELD=(ALTERED_").append(field.getName()).append("=").append(field.getExpression()).append(field.getName()).append("), TYPE=ASCII, POSITION=").append(count).append(", ODEF=\"").append(field.getName()).append("\", SEPARATOR=\"\\t\")\n");
            } else {
                sb.append("/FIELD=(").append(field.getName()).append(", TYPE=ASCII, POSITION=").append(count).append(", SEPARATOR=\"\\t\")\n");
            }
        }
        return sb.toString();
    }

    public static void clearOut(AtomicReference<Integer> i) throws IOException {
        for (SclScript script : scripts.get().values()) {
            script.getStdin().close();
        }
        scripts.set(new HashMap<>());
        i.set(0);
    }

    public static void classify(Set<Map.Entry<String, JsonElement>> values, DataClassLibrary dataClassLibrary, ArrayList<SclField> fields) {
        int count = 0;
        for (Map.Entry<String, JsonElement> value : values) {
            for (Map.Entry<String, DataMatcher> entry : dataClassLibrary.dataMatcherMap.entrySet()) {
                if (entry.getValue().isMatch(value.getValue().getAsString())) {
                    fields.get(count).expressionApplied = true;
                    fields.get(count).expression = entry.getKey();
                }
            }
            count++;
        }
    }


    public static void main(String[] args) throws Exception {
        ArrayList<String> columns = new ArrayList<>();
        Logger LOG = LoggerFactory.getLogger(test.class);
        LOG.info("Launching Debezium embedded engine");
        Properties props;
        try (InputStream input = new FileInputStream("config.properties")) {

            props = new Properties();

            // load a properties file
            props.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
            Configuration config = Configuration.empty();
            props = config.asProperties();
            props.setProperty("name", "engine");
            props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
            props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
            props.setProperty("offset.storage.file.filename", "offsets.dat");
            props.setProperty("offset.flush.interval.ms", "1000");
            /* begin connector properties */
            props.setProperty("database.hostname", "localhost");
            props.setProperty("database.port", "3306");
            props.setProperty("database.user", "devonk");
            if (System.getenv("DBPASS") == null) {
                throw new Exception("Set environment variable for DBPASS with the password to the database.");
            }
            props.setProperty("database.password", System.getenv("DBPASS"));
            props.setProperty("database.server.id", "85744");
            props.setProperty("database.server.name", "test");
            props.setProperty("database.history",
                    "io.debezium.relational.history.FileDatabaseHistory");
            props.setProperty("database.history.file.filename",
                    "dbhistory.dat");
            props.setProperty("table.exclude.list", ".*_masked");
            props.setProperty("secondsToClearout", "60");
        }
        DataClassLibrary dataClassLibrary = new DataClassLibrary("iriLibrary.dataclass");
        //  RulesLibrary rulesLibrary = new RulesLibrary("iriLibrary.rules");

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
                            if (operation.equals("c")) {
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
                                            if (count == scripts.get().size()) {
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
                                    try {
                                        FileWriter myWriter = new FileWriter(tempFile);
                                        scripts.get().put(i.get().toString(), new SclScript(jsonObject.get("payload").getAsJsonObject().get("source").getAsJsonObject().get("table").getAsString(), "localmysql", columns));
                                        classify(Jobject_.entrySet(), dataClassLibrary, scripts.get().get(i.get().toString()).getFields());
                                        myWriter.write(sortCLScript(scripts.get().get(i.get().toString())));
                                        myWriter.close();
                                        LOG.info("Successfully wrote SortCL script to the temporary file.");
                                    } catch (IOException e) {
                                        System.out.println("An error occurred.");
                                        e.printStackTrace();
                                    }

                                    try {
                                        scripts.get().get(i.toString()).setProcess(new ProcessBuilder("sortcl", "/SPEC=" + tempFile.getAbsolutePath()).redirectErrorStream(true).start());
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }


                                int ct = 0;
                                for (Map.Entry<String, JsonElement> jj : Jobject_.entrySet()) {
                                    ct++;
                                    LOG.info(String.valueOf(jj.getValue()));
                                    try {
                                        scripts.get().get(Integer.toString(count))
                                                .getStdin().write(jj.getValue().getAsString());
                                        if (ct < Jobject_.entrySet().size()) {
                                            scripts.get().get(Integer.toString(count)).getStdin().write("\t");
                                        }
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                }

                                try {
                                    scripts.get().get(Integer.toString(count)).getStdin().newLine();
                                    scripts.get().get(Integer.toString(count)).getStdin().flush();
                                    //script.get().getStdin().close();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }

                                columns.clear();
                                i.set(i.get() + 1);
                            }
                        } catch (NullPointerException ee) {
                            ee.printStackTrace();
                        }
                    }
                })
                .build()) {
            ScheduledThreadPoolExecutor dispenser = new ScheduledThreadPoolExecutor(1);
            dispenser.scheduleAtFixedRate(() -> {
                try {
                    clearOut(i);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, 0, Long.parseLong(props.getProperty("secondsToClearout")), TimeUnit.SECONDS);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.execute(engine);

        }
    }
}

