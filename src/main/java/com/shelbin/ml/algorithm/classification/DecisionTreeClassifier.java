package com.shelbin.ml.algorithm.classification;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

@Slf4j
public class DecisionTreeClassifier extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.classification.DecisionTreeClassifier()
                .setMaxDepth(algorithm.getInteger("maxDepth"))
                .setMaxBins(algorithm.getInteger("maxBins"))
                .setMinInfoGain(algorithm.getDouble("minInfoGain"))
                .setImpurity(algorithm.getString("impurity"))
                .setMinInstancesPerNode(algorithm.getInteger("minInstancesPerNode"))
                .setMaxMemoryInMB(algorithm.getInteger("maxMemoryInMB"))
                .setCheckpointInterval(algorithm.getInteger("checkpointInterval"));
    }

}