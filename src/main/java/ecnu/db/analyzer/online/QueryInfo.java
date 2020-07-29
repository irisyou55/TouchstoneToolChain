package ecnu.db.analyzer.online;

import ecnu.db.constraintchain.filter.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lianxuechao
 */
public class QueryInfo {
    String data;
    String tableName;
    int lastNodeLineCount;
    List<Parameter> parameters = new ArrayList<>();

    public QueryInfo(String data, String tableName, int lastNodeLineCount) {
        this.data = data;
        this.lastNodeLineCount = lastNodeLineCount;
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getData() {
        return data;
    }

    public void addConstraint(String data) {
        this.data += data;
    }

    public int getLastNodeLineCount() {
        return lastNodeLineCount;
    }

    public void setLastNodeLineCount(int lastNodeLineCount) {
        this.lastNodeLineCount = lastNodeLineCount;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    @Override
    public String toString() {
        return "QueryInfo{" +
                "data='" + data + '\'' +
                ", tableName='" + tableName + '\'' +
                '}';
    }
}
