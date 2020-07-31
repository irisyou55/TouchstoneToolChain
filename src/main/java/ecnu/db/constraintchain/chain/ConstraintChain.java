package ecnu.db.constraintchain.chain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.constraintchain.filter.Parameter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final String tableName;
    private final List<ConstraintChainNode> nodes = new ArrayList<>();
    private List<Parameter> parameters = new ArrayList<>();

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    public String getTableName() {
        return tableName;
    }

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    @JsonIgnore
    public List<Parameter> getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }
}
