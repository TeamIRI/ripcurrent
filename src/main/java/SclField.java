public class SclField {
    String name;
    Boolean expressionApplied;
    String expression;

    SclField(String name) {
        this.name = name;
        this.expressionApplied = false;

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
}
