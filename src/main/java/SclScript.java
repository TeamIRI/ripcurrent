/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Class to represent a SortCL script, or at least the attributes of a SortCL script pertinent to this application.
 *
 * Contributors:
 *     devonk
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Path;
import java.util.ArrayList;

public class SclScript {
    String sourceTableIdentifier;
    String targetTableIdentifier;
    String table;
    String key;
    String schema;
    String DSN;
    String operation;
    String target;
    String targetProcessType;
    String postfix;
    ArrayList<SclField> fields = new ArrayList<>();
    Process process;
    BufferedWriter stdin;
    BufferedReader stderr;
    BufferedReader stdout;

    // Constructor for just targeting a database.
    SclScript(String sourceTable, String sourceSchema, String targetSchema, String DSN, ArrayList<String> fields, String operation, String postfixTableString) {
        this.operation = operation;
        if (targetSchema != null && targetSchema.length() > 0) {
            this.targetTableIdentifier = targetSchema + "." + sourceTable + postfixTableString;
        } else {
            this.targetTableIdentifier = sourceTable + postfixTableString;
        }
        this.sourceTableIdentifier = sourceSchema + "." + sourceTable;
        this.DSN = DSN;
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
        this.targetProcessType = "ODBC";
        this.postfix = postfixTableString;
    }

    // Constructor for just targeting files.
    SclScript(String sourceTable, String sourceSchema, ArrayList<String> fields, String operation, String targetProcessType, Path target, String postfix) {
        this.operation = operation;
        Path parent = target.getParent();
        Path file = target.getFileName();
        String parentString = ".";
        if (parent != null) {
            parentString = parent.toString();
        }
        String fileString = "";
        if (file != null) {
            fileString = file.toString();
        }
        this.target = parentString + "/" + sourceSchema + "_" + sourceTable + "-" + postfix + "-" + fileString;
        this.sourceTableIdentifier = sourceSchema + "." + sourceTable;
        if (targetProcessType != null) {
            this.targetProcessType = targetProcessType;
        } else {
            this.targetProcessType = "RECORD";
        }
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
        this.postfix = postfix;
    }

    // Constructor for targeting a database and files at the same time.
    SclScript(String sourceTable, String sourceSchema, String targetSchema, ArrayList<String> fields, String operation, String targetProcessType, Path target, String postfix, String DSN) {
        this.operation = operation;
        Path parent = target.getParent();
        Path file = target.getFileName();
        String parentString = ".";
        if (parent != null) {
            parentString = parent.toString();
        }
        String fileString = "";
        if (file != null) {
            fileString = file.toString();
        }
        if (targetSchema != null && targetSchema.length() > 0) {
            this.targetTableIdentifier = targetSchema + "." + sourceTable + postfix;
        } else {
            this.targetTableIdentifier = sourceTable + postfix;
        }
        this.target = parentString + "/" + sourceSchema + "_" + sourceTable + "-" + postfix + "-" + fileString;
        this.sourceTableIdentifier = sourceSchema + "." + sourceTable;
        this.targetProcessType = targetProcessType;
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
        this.DSN = DSN;
        this.postfix = postfix;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getTargetProcessType() {
        return targetProcessType;
    }

    public void setTargetProcessType(String targetProcessType) {
        this.targetProcessType = targetProcessType;
    }

    public ArrayList<SclField> getFields() {
        return fields;
    }

    public void setFields(ArrayList<SclField> fields) {
        this.fields = fields;
    }

    public String getDSN() {
        return DSN;
    }

    public void setDSN(String DSN) {
        this.DSN = DSN;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Process getProcess() {
        return process;
    }

    public void setProcess(Process process) {
        this.process = process;
        this.stdin = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
        this.stderr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public BufferedReader getStderr() {
        return stderr;
    }

    public BufferedReader getStdout() {
        return stdout;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTargetTableIdentifier() {
        return targetTableIdentifier;
    }

    public void setTargetTableIdentifier(String targetTableIdentifier) {
        this.targetTableIdentifier = targetTableIdentifier;
    }

    public String getSourceTableIdentifier() {
        return sourceTableIdentifier;
    }

    public void setSourceTableIdentifier(String sourceTableIdentifier) {
        this.sourceTableIdentifier = sourceTableIdentifier;
    }

    public String getPostfix() {
        return postfix;
    }

    public void setPostfix(String postfix) {
        this.postfix = postfix;
    }

    public BufferedWriter getStdin() {
        return stdin;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
