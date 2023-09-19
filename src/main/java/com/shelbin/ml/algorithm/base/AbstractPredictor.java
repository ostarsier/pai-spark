package com.shelbin.ml.algorithm.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.feature.RFormula;


/**
 * 监督学习：分类、回归算法模型组件
 * doExecute: 指定特征和标签列，构造并训练模型
 * <p>
 * 输入：训练数据、[验证数据]
 * 输出：模型
 */
@Slf4j
public abstract class AbstractPredictor extends AbstractComponent {

    @Override
    protected void before() throws Exception {
        super.before();
        // todo 验证数据
    }

    @Override
    public void doExecute() throws Exception {
        //指定特征和标签列
        RFormula rFormula = new RFormula().setFormula(formula());
        Predictor predictor = predictor();
        predictor.setFeaturesCol(rFormula.getFeaturesCol());
        predictor.setLabelCol(rFormula.getLabelCol());

        // 3、构建并训练模型
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{rFormula, predictor});
        this.pipelineModel = pipeline.fit(inputData);
    }


    protected abstract Predictor predictor();

}