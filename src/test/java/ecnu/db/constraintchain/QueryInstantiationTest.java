package ecnu.db.constraintchain;

import ch.obermuhlner.math.big.BigDecimalMath;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainReader;
import ecnu.db.constraintchain.filter.operation.AbstractFilterOperation;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class QueryInstantiationTest {
    @Test
    public void getOperationsTest() throws Exception {
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/constraintChain.json");
        Multimap<String, AbstractFilterOperation> query2operations = ArrayListMultimap.create();
        for (String query : query2chains.keySet()) {
            List<ConstraintChain> chains = query2chains.get(query);
            for (ConstraintChain chain: chains) {
                String tableName = chain.getTableName();
                for (ConstraintChainNode node: chain.getNodes()) {
                    if (node instanceof ConstraintChainFilterNode) {
                        List<AbstractFilterOperation> operations = ((ConstraintChainFilterNode) node).pushDownProbability();
                        query2operations.putAll(query + "_" + tableName, operations);
                    }
                }
            }
        }
        List<AbstractFilterOperation> operations;
        operations = new ArrayList<>(query2operations.get("2.sql_1_tpch.part"));
        assertEquals(2, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.00416), BigDecimal.valueOf(0.5), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("3.sql_1_tpch.customer"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.20126), Matchers.comparesEqualTo(operations.get(0).getProbability()));
        operations = new ArrayList<>(query2operations.get("3.sql_1_tpch.orders"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.4827473333), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("11.sql_1_tpch.nation"));
        assertEquals(1, operations.size());
        assertThat(BigDecimal.valueOf(0.04), Matchers.comparesEqualTo(operations.get(0).getProbability()));

        operations = new ArrayList<>(query2operations.get("6.sql_1_tpch.lineitem"));
        assertEquals(3, operations.size());
        assertThat(BigDecimalMath.pow(BigDecimal.valueOf(0.01904131080122942), BigDecimal.ONE.divide(BigDecimal.valueOf(3), BIG_DECIMAL_DEFAULT_PRECISION), BIG_DECIMAL_DEFAULT_PRECISION), Matchers.comparesEqualTo(operations.get(0).getProbability()));

    }

    @Test
    public void computeTest() throws Exception {
        Map<String, List<ConstraintChain>> query2chains = ConstraintChainReader.readConstraintChain("src/test/resources/data/query-instantiation/constraintChain.json");
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(AbstractColumn.class, new ColumnDeserializer());
        mapper.registerModule(module);
        mapper.findAndRegisterModules();
        Map<String, Schema> schemas = mapper.readValue(
                FileUtils.readFileToString(new File("src/test/resources/data/query-instantiation/schema.json"), UTF_8),
                new TypeReference<HashMap<String, Schema>>() {});
        QueryInstantiation.compute(query2chains.values().stream().flatMap(Collection::stream).collect(Collectors.toList()), schemas);
    }
}