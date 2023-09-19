package com.shelbin.ml.algorithm.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;

import static com.shelbin.ml.algorithm.bean.AnchorConstants.OUTPUT_TABLE;

/**
 * 可输出模型的数据转换组件
 * 输入：数据
 * 输出：模型、数据
 */
@Slf4j
public abstract class AbstractEstimator extends AbstractComponent {

    @Override
    public void doExecute() throws Exception {
        Estimator estimator = estimator();
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{estimator});
        this.pipelineModel = pipeline.fit(inputData);
        if (anchor.getString(OUTPUT_TABLE) != null) {
            this.outputData = this.pipelineModel.transform(inputData);
        }
    }


    protected abstract Estimator estimator();

}