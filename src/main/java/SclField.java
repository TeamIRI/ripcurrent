public class SclField {
    String name;
    Boolean expressionApplied;
    String expression;
    String dataType;

    SclField(String name) {
        this.name = name;
        this.expressionApplied = false;
        this.dataType = "ASCII";
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
}
