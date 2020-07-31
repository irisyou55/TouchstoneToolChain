package ecnu.db.analyzer.online.node;

import ecnu.db.tidb.TidbNodeTypeTool;

/**
 * @author lianxuechao
 */
public class NodeTypeRefFactory {
    public static NodeTypeTool getNodeTypeRef(String tidbVersion) {
        return new TidbNodeTypeTool();
    }
}
