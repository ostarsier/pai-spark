package com.shelbin.ml.algorithm.prediction;

import com.shelbin.ml.algorithm.base.AbstractComponent;
import com.google.common.collect.ObjectArrays;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;

/**
 * 处理前输出的列
 * +------------+-----------+------------+-----------+----------+--------------------+-----+--------------------+--------------------+----------+
 * |Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|   Species|            features|label|       rawPrediction|         probability|prediction|
 * +------------+-----------+------------+-----------+----------+--------------------+-----+--------------------+--------------------+----------+
 * |         5.1|        3.5|         1.4|        0.2|    setosa|(36,[5,18,19,29],...|  1.0|[-8.4501977554307...|[1.38883762658331...|       1.0|
 * 处理后输出的列
 * 特征列1 | 特征列2 | 特征列3...| label | prediction |rawPrediction_0 |rawPrediction_1 |probability_0 |probability_1
 */
public class Prediction extends AbstractComponent implements Serializable {

    @Override
    public void doExecute() {

        // 预测并保存结果
        PipelineModel pipelineModel = PipelineModel.load(inputModelPath());
        Dataset<Row> predictions = pipelineModel.transform(inputData);

        // 回归
        if (!Arrays.asList(predictions.schema().fieldNames()).contains("rawPrediction")) {
            String[] resultCols = ObjectArrays.concat(featureCols(), new String[]{"label", "prediction"}, String.class);
            this.outputData = predictions.selectExpr(resultCols);
            return;
        }

        //分类
        // 自定义UDF函数来提取rawPrediction、probability的每个项
        UDF2<Vector, Integer, Double> extractVector = (vector, index) -> vector.toArray()[index];
        spark.udf().register("extractVector", extractVector, DataTypes.DoubleType);


        Row first = predictions.selectExpr("rawPrediction").first();
        int classificationSize = ((DenseVector) first.get(0)).size();

        String[] resultCols = ObjectArrays.concat(featureCols(), new String[]{"label", "prediction", "rawPrediction", "probability"}, String.class);
        predictions = predictions.selectExpr(resultCols);
        for (int i = 0; i < classificationSize; i++) {
            predictions = predictions.withColumn("rawPrediction_" + i, functions.callUDF("extractVector", predictions.col("rawPrediction"), lit(i)));
        }

        for (int i = 0; i < classificationSize; i++) {
            predictions = predictions.withColumn("probability_" + i, functions.callUDF("extractVector", predictions.col("probability"), lit(i)));
        }

        predictions = predictions.drop("rawPrediction", "probability");

        this.outputData = predictions;
    }

}