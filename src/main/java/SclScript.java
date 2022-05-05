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
    String table;
    String DSN;
    String operation;
    String target;
    String targetProcessType;
    ArrayList<SclField> fields = new ArrayList<>();
    Process process;
    BufferedWriter stdin;
    BufferedReader stderr;
    BufferedReader stdout;

    SclScript(String table, String DSN, ArrayList<String> fields, String operation, String postfixTableString) {
        this.operation = operation;
        this.table = table + postfixTableString;
        this.DSN = DSN;
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
        this.targetProcessType = "ODBC";
    }

    SclScript(String table, ArrayList<String> fields, String operation, String targetProcessType, Path target, String postfix) {
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
        this.target = parentString + "/" + table + "-" + postfix + fileString;
        this.table = table + postfix;
        this.targetProcessType = targetProcessType;
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
    }

    SclScript(String table, ArrayList<String> fields, String operation, String targetProcessType, Path target, String postfix, String DSN) {
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
        this.target = parentString + "/" + table + "-" + postfix + fileString;
        this.table = table + postfix;
        this.targetProcessType = targetProcessType;
        for (String fieldName : fields) {
            this.fields.add(new SclField(fieldName));
        }
        this.DSN = DSN;
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

    public BufferedWriter getStdin() {
        return stdin;
    }
}
