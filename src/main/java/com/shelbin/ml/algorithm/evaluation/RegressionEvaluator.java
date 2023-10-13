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

public class RegressionEvaluator extends AbstractComponent {

    @Override
    public void doExecute() throws Exception {
        JSONObject metrics = new JSONObject();
        org.apache.spark.ml.evaluation.RegressionEvaluator evaluator = new org.apache.spark.ml.evaluation.RegressionEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction");

        List<String> metricNames = Lists.newArrayList("rmse", "mse", "r2", "mae", "var");
        for (String metricName : metricNames) {
            double metricValue = evaluator.setMetricName(metricName).evaluate(inputData);
            metrics.put(metricName, metricValue);
        }


        inputData.createOrReplaceTempView("predictions");
        // 计算MAPE（平均绝对百分比误差）
        double mape = spark.sql("SELECT AVG(ABS(prediction - label) / label * 100) AS MAPE FROM predictions").first().getDouble(0);

        // 计算R（相关系数）
        double r = spark.sql("SELECT CORR(prediction, label) AS R FROM predictions").first().getDouble(0);

        // 计算SAE（总绝对误差）
        double sae = spark.sql("SELECT SUM(ABS(prediction - label)) AS SAE FROM predictions").first().getDouble(0);

        // 计算SSE（残差平方和）
        double sse = spark.sql("SELECT SUM(POW(prediction - label, 2)) AS SSE FROM predictions").first().getDouble(0);

        // 计算SSR（回归平方和）
        double ssr = spark.sql("SELECT SUM(POW(prediction - label, 2)) AS SSR FROM predictions").first().getDouble(0);

        // 获取数据行数
        long count = spark.sql("SELECT COUNT(*) AS count FROM predictions").first().getLong(0);

        // 获取预测值的平均值
        double predictionMean = spark.sql("SELECT AVG(prediction) AS predictionMean FROM predictions").first().getDouble(0);

        // 获取真实值的平均值
        double yMean = spark.sql("SELECT AVG(label) AS yMean FROM predictions").first().getDouble(0);

        // 计算SST（总平方和）
        double sst = spark.sql("SELECT SUM(POW(label - " + yMean + ", 2)) AS SST FROM predictions").first().getDouble(0);

        double mad = spark.sql("SELECT AVG(ABS(label - prediction)) AS mad  FROM predictions").first().getDouble(0);

        metrics.put("mape", mape);
        metrics.put("r", r);
        metrics.put("sae", sae);
        metrics.put("count", count);
        metrics.put("predictionMean", predictionMean);
        metrics.put("sse", sse);
        metrics.put("ssr", ssr);
        metrics.put("yMean", yMean);
        metrics.put("sst", sst);
        metrics.put("mad", mad);

        StructType schema = new StructType()
                .add("parameter", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("value", DataTypes.StringType);

        List<String> keys = Lists.newArrayList("sst", "sse", "ssr", "r2", "r", "mse", "rmse", "mae", "mad", "mape", "count", "yMean", "predictionMean");
        List<String> keysCn = Lists.newArrayList("总平方和", "误差平方和", "回归平方和", "判定系数", "多重相关系数", "均方误差", "均方根误差", "平均绝对误差", "平均误差", "平均绝对百分误差", "行数", "原始因变量的均值", "预测结果的均值");

        Row[] rows = new Row[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            String desc = keysCn.get(i);
            String value = metrics.getString(key);
            rows[i] = RowFactory.create(key, desc, value);
        }

        this.outputData = spark.createDataFrame(Arrays.asList(rows), schema);

    }

}
