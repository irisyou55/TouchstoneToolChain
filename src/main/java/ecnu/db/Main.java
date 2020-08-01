package ecnu.db;


import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.statical.QueryReader;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.dbconnector.DumpFileConnector;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.schema.AbstractSchemaGenerator;
import ecnu.db.schema.Schema;
import ecnu.db.tidb.TidbAnalyzer;
import ecnu.db.tidb.TidbConnector;
import ecnu.db.tidb.TidbSchemaGenerator;
import ecnu.db.utils.SqlTemplateHelper;
import ecnu.db.utils.StorageManager;
import ecnu.db.utils.SystemConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.INIT_HASHMAP_SIZE;

/**
 * @author wangqingshuai
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        SystemConfig config = SystemConfig.readConfig(args[0]);
        List<File> files = Optional.ofNullable(new File(config.getSqlsDirectory()).listFiles())
                .map(Arrays::asList)
                .orElse(new ArrayList<>())
                .stream()
                .filter((file) -> file.isFile() && file.getName().endsWith(".sql"))
                .collect(Collectors.toList());

        StorageManager storageManager = new StorageManager(config.getResultDirectory(), config.getDumpDirectory(), config.getLoadDirectory(), config.getLogDirectory());
        storageManager.init();

        DatabaseConnectorInterface dbConnector;
        Map<String, Schema> schemas = new HashMap<>(INIT_HASHMAP_SIZE);
        List<String> tableNames;
        if (storageManager.isLoad()) {
            // todo 从文件中正确加载表信息
            tableNames = storageManager.loadTableNames();
            logger.info("加载表名成功，表名为:" + tableNames);
            Map<String, List<String[]>> queryPlanMap = storageManager.loadQueryPlans();
            Map<String, Integer> multiColNdvMap = storageManager.loadMultiColMap();
            schemas = storageManager.loadSchemas();
            dbConnector = new DumpFileConnector(queryPlanMap, multiColNdvMap);
            logger.info("数据加载完毕");
        } else {
            switch (config.getDatabaseVersion()) {
                case TiDB3:
                case TiDB4:
                    dbConnector = new TidbConnector(config);
                    tableNames = ((AbstractDbConnector) dbConnector).fetchTableNames(config.isCrossMultiDatabase(),
                            config.getDatabaseName(), files, JdbcConstants.MYSQL);
                    logger.info("获取表名成功，表名为:" + tableNames);
                    AbstractSchemaGenerator dbSchemaGenerator = new TidbSchemaGenerator();
                    for (String canonicalTableName : tableNames) {
                        Schema schema = ((AbstractDbConnector) dbConnector).fetchSchema(dbSchemaGenerator, canonicalTableName);
                        schemas.put(canonicalTableName, schema);
                    }
                    Schema.initFks(((AbstractDbConnector) dbConnector).databaseMetaData, schemas);
                    logger.info("获取表结构和数据分布成功");
                    break;
                default:
                    throw new UnsupportedDBTypeException(config.getDatabaseVersion());
            }
        }
        AbstractAnalyzer queryAnalyzer = getAnalyzer(config, dbConnector, schemas);
        Map<String, List<ConstraintChain>> queryInfos = new HashMap<>();
        String staticalDbType = queryAnalyzer.getStaticalDbVersion();
        boolean needLog = false;
        logger.info("开始获取查询计划");
        for (File sqlFile : files) {
            if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                List<String> queries = QueryReader.getQueriesFromFile(sqlFile.getPath(), staticalDbType);
                int index = 0;
                List<String[]> queryPlan = new ArrayList<>();
                for (String query : queries) {
                    index++;
                    String queryCanonicalName = String.format("%s_%d", sqlFile.getName(), index);
                    try {
                        logger.info(String.format("%-15s Status:开始获取", queryCanonicalName));
                        queryPlan = queryAnalyzer.getQueryPlan(queryCanonicalName, query);
                        if (storageManager.isDump()) {
                            storageManager.dumpQueryPlan(queryPlan, queryCanonicalName);
                        }
                        ExecutionNode root = queryAnalyzer.getExecutionTree(queryPlan);
                        List<ConstraintChain> constraintChains = queryAnalyzer.extractQueryInfos(queryCanonicalName, root);
                        queryInfos.put(queryCanonicalName, constraintChains);
                        List<Parameter> parameters = constraintChains.stream().flatMap((c -> c.getParameters().stream())).collect(Collectors.toList());
                        logger.info(String.format("%-15s Status:获取成功", queryCanonicalName));
                        query = SqlTemplateHelper.templatizeSql(queryCanonicalName, query, staticalDbType, parameters);
                        storageManager.storeSqlResult(sqlFile, query, staticalDbType);
                    } catch (TouchstoneToolChainException e) {
                        logger.error(String.format("%-15s Status:获取失败", queryCanonicalName), e);
                        needLog = true;
                        if (queryPlan != null && !queryPlan.isEmpty()) {
                            storageManager.logQueryPlan(queryPlan, queryCanonicalName);
                            logger.info(String.format("失败的query %s的查询计划已经存盘到'%s'", queryCanonicalName, storageManager.getLogDir().getAbsolutePath()));
                        }
                    }
                }
            }
        }
        logger.info("获取查询计划完成");
        if (storageManager.isDump()) {
            storageManager.dumpStaticInfo(dbConnector.getMultiColNdvMap(), schemas, tableNames);
            logger.info("表结构和数据分布持久化成功");
        }
        if (needLog) {
            storageManager.logStaticInfo(dbConnector.getMultiColNdvMap(), schemas, tableNames);
            logger.info(String.format("关于表的日志信息已经存盘到'%s'", storageManager.getLogDir().getAbsolutePath()));
        }
        storageManager.storeSchemaResult(schemas);
        storageManager.storeConstrainChainResult(queryInfos);
    }

    private static AbstractAnalyzer getAnalyzer(SystemConfig config, DatabaseConnectorInterface dbConnector, Map<String, Schema> schemas) throws UnsupportedDBTypeException {
        Multimap<String, String> tblName2CanonicalTblName = ArrayListMultimap.create();
        for (String canonicalTableName : schemas.keySet()) {
            tblName2CanonicalTblName.put(canonicalTableName.split("\\.")[1], canonicalTableName);
        }
        return new TidbAnalyzer(config, dbConnector, schemas, tblName2CanonicalTblName);
    }


}
