package com.shelbin.ml.algorithm.classification;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

@Slf4j
public class RandomForestClassifier extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.classification.RandomForestClassifier()
                .setMaxDepth(algorithm.getInteger("maxDepth"))
                .setMaxBins(algorithm.getInteger("maxBins"))
                .setMinInfoGain(algorithm.getDouble("minInfoGain"))
                .setImpurity(algorithm.getString("impurity"))
                .setMinInstancesPerNode(algorithm.getInteger("minInstancesPerNode"))
                .setMaxMemoryInMB(algorithm.getInteger("maxMemoryInMB"))
                .setCheckpointInterval(algorithm.getInteger("checkpointInterval"))
                .setNumTrees(algorithm.getInteger("numTrees"))
                .setFeatureSubsetStrategy(algorithm.getString("featureSubsetStrategy"))
                .setSubsamplingRate(algorithm.getDouble("subsamplingRate"));

    }

}