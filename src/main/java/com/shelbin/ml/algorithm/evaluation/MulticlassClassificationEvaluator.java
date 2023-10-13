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

public class MulticlassClassificationEvaluator extends AbstractComponent {

    @Override
    public void doExecute() throws Exception {
        inputData.createOrReplaceTempView("predictions");

        JSONObject metrics = new JSONObject();
        org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator evaluator = new org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction");

        List<String> metricNames = Lists.newArrayList("f1", "accuracy", "weightedPrecision", "weightedRecall", "weightedTruePositiveRate", "weightedFalsePositiveRate"
                , "weightedFMeasure", "truePositiveRateByLabel", "falsePositiveRateByLabel", "precisionByLabel", "recallByLabel", "fMeasureByLabel", "hammingLoss");
        List<String> metricNamesCn = Lists.newArrayList("F1值", "准确率", "加权精确率", "加权召回率", "加权真阳性率",
                "加权假阳性率", "加权F1值", "各类别真阳性率", "各类别假阳性率", "各类别精确率", "各类别召回率", "各类别F1值", "汉明损失");

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
