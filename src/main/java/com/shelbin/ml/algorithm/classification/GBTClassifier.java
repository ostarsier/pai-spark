package com.shelbin.ml.algorithm.classification;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

/**
 * 梯度提升树分类
 */
@Slf4j
public class GBTClassifier extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.classification.GBTClassifier()
                .setFeatureSubsetStrategy(algorithm.getString("featureSubsetStrategy"))
                .setImpurity(algorithm.getString("impurity"))
                .setMaxBins(algorithm.getInteger("maxBins"))
                .setMaxDepth(algorithm.getInteger("maxDepth"))
                .setMaxIter(algorithm.getInteger("maxIter"))
                .setMinInfoGain(algorithm.getDouble("minInfoGain"))
                .setMinInstancesPerNode(algorithm.getInteger("minInstancesPerNode"))
                .setStepSize(algorithm.getInteger("stepSize"))
                .setSubsamplingRate(algorithm.getDouble("subsamplingRate"));
    }

}