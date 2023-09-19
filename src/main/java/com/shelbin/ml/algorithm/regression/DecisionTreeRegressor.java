package com.shelbin.ml.algorithm.regression;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

@Slf4j
public class DecisionTreeRegressor extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.regression.DecisionTreeRegressor()
                .setMaxBins(algorithm.getInteger("maxBins"))
                .setMaxDepth(algorithm.getInteger("maxDepth"))
                .setMinInfoGain(algorithm.getDouble("minInfoGain"))
                .setMinInstancesPerNode(algorithm.getInteger("minInstancesPerNode"))
                .setCheckpointInterval(algorithm.getInteger("checkpointInterval"));
    }

}