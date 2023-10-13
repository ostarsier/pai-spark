package com.shelbin.ml.algorithm.evaluation;

import com.alibaba.fastjson2.JSONObject;
import com.shelbin.ml.algorithm.base.AbstractComponent;
import com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class BinaryClassificationEvaluator extends AbstractComponent {

    @Override
    public void doExecute() throws Exception {
        inputData.createOrReplaceTempView("predictions");

        JSONObject metrics = new JSONObject();
        org.apache.spark.ml.evaluation.BinaryClassificationEvaluator evaluator = new org.apache.spark.ml.evaluation.BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("prediction");

        List<String> metricNames = Lists.newArrayList("areaUnderROC", "areaUnderPR");
        List<String> metricNamesCn = Lists.newArrayList("ROC曲线下面积", "PR曲线下面积");
        for (String metricName : metricNames) {
            double metricValue = evaluator.setMetricName(metricName).evaluate(inputData);
            metrics.put(metricName, metricValue);
        }


        StructType schema = new StructType()
                .add("parameter", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("value", DataTypes.StringType);

        Row[] rows = new Row[metricNames.size()];
        for (int i = 0; i < metricNames.size(); i++) {
            String key = metricNames.get(i);
            String desc = metricNamesCn.get(i);
            String value = metrics.getString(key);
            rows[i] = RowFactory.create(key, desc, value);
        }

        this.outputData = spark.createDataFrame(Arrays.asList(rows), schema);

    }

}
