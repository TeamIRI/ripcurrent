import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

public class SclScript {
    String table;
    String DSN;
    ArrayList<String> fields;
    Process process;
    BufferedWriter stdin;
    BufferedReader stderr;
    BufferedReader stdout;

    SclScript(String table, String DSN, ArrayList<String> fields) {
        this.table = table + "_masked";
        this.DSN = DSN;
        this.fields = fields;
    }

    public ArrayList<String> getFields() {
        return fields;
    }

    public void setFields(ArrayList<String> fields) {
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
