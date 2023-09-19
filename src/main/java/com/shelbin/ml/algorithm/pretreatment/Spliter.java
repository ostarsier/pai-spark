package com.shelbin.ml.algorithm.pretreatment;

import com.shelbin.ml.algorithm.base.AbstractComponent;
import com.shelbin.ml.algorithm.bean.AnchorConstants;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * 拆分训练数据和测试数据
 */
public class Spliter extends AbstractComponent {
    private Dataset<Row> trainingData;
    private Dataset<Row> testData ;

    @Override
    public void doExecute() {
        double ratio = algorithm.getDouble("ratio");
        Dataset<Row>[] splits = inputData.randomSplit(new double[]{ratio, 1 - ratio});
        this.trainingData = splits[0];
        this.testData = splits[1];
    }

    @Override
    protected void after() throws Exception {
        write(trainingData, anchor.getString(AnchorConstants.OUTPUT_TABLE_1));
        write(testData, anchor.getString(AnchorConstants.OUTPUT_TABLE_2));
    }

}