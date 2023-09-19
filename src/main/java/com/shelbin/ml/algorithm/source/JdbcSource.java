package com.shelbin.ml.algorithm.source;

import com.shelbin.ml.algorithm.base.AbstractComponent;
import com.shelbin.ml.algorithm.bean.JdbcConstants;

public class JdbcSource extends AbstractComponent {

    @Override
    protected void before() throws Exception {
        String schemaName = algorithmIo.getString(JdbcConstants.SCHEMA_NAME);
        String tableName = algorithmIo.getString(JdbcConstants.TABLE_NAME);
        this.inputData = spark.read()
                .format("jdbc")
                .option("url", algorithmIo.getString(JdbcConstants.URL))
                .option("dbtable", String.format("`%s`.%s", schemaName, tableName))
                .option("user", algorithmIo.getString(JdbcConstants.USER_NAME))
                .option("password", algorithmIo.getString(JdbcConstants.PASSWORD))
                .load();
    }

    @Override
    public void doExecute() throws Exception {
        this.outputData = this.inputData;
    }

}
