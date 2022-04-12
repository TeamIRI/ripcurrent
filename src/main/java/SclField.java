/*
 * Copyright (c) 2022 Innovative Routines International (IRI), Inc.
 *
 * Description: Class to represent a field in a SortCL field in a SortCL script, or at least the attributes of a SortCL field pertinent to this application.
 *
 * Contributors:
 *     devonk
 */
public class SclField {
    String name;
    Boolean expressionApplied;
    String expression;
    String dataType;
    Integer precision;

    SclField(String name) {
        this.name = name;
        this.expressionApplied = false;
        this.dataType = "ASCII";
        this.precision = -1;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Boolean getExpressionApplied() {
        return expressionApplied;
    }

    public void setExpressionApplied(Boolean expressionApplied) {
        this.expressionApplied = expressionApplied;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }
}
