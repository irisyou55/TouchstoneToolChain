package ecnu.db.constraintchain.chain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeDeserializer;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprNodeDeserializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author wangqingshuai
 */
public class ConstraintChainReader {

    /**
     * 没有找到文件时抛出IO异常
     *
     * @return 加载成功的约束链
     */
    public static Map<String, List<ConstraintChain>> readConstraintChain(String fileName) throws IOException {
        String content = FileUtils.readFileToString(new File(fileName), UTF_8);
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(ConstraintChainNode.class, new ConstraintChainNodeDeserializer());
        module.addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer());
        module.addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer());
        mapper.registerModule(module);
        return mapper.readValue(content, new TypeReference<Map<String, List<ConstraintChain>>>() {});
    }

}
