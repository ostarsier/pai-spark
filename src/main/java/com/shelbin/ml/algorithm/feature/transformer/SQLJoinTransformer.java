package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractComponent;
import com.shelbin.ml.algorithm.bean.JdbcConstants;
import com.shelbin.ml.algorithm.exception.AlgorithmException;
import com.shelbin.ml.algorithm.utils.SqlParserUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SaveMode;

import java.util.List;
import java.util.Properties;

/**
 * SQL融合：将用户的select 写到hdfs和jdbc表
 */
@Slf4j
public class SQLJoinTransformer extends AbstractComponent {

    private String url;
    private Properties properties = new Properties();

    @Override
    protected void before() throws Exception {
        this.url = algorithmIo.getString(JdbcConstants.URL);

        properties.put("user", algorithmIo.getString(JdbcConstants.USER_NAME));
        properties.put("password", algorithmIo.getString(JdbcConstants.PASSWORD));

    }

    @Override
    public void doExecute() throws Exception {

        String statement = algorithm.getString("statement");

        //注册statement中的表
        List<String> tableNameList = SqlParserUtils.parseTables(statement);
        if (tableNameList.isEmpty()) {
            throw new AlgorithmException("SQL融合语句中没有表名");
        }

        for (String tableName : tableNameList) {
            spark.read().jdbc(url, tableName, properties).createOrReplaceTempView(tableName);
        }

        this.outputData = spark.sql(statement);

    }

    @Override
    protected void after() throws Exception {
        super.after();
        String outputTable = algorithm.getString("outputTable");
        if (outputTable != null) {
            outputData.write().mode(SaveMode.Overwrite).jdbc(url, outputTable, properties);
        }
    }
}