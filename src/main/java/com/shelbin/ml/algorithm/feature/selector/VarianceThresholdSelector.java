package com.shelbin.ml.algorithm.feature.selector;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class VarianceThresholdSelector extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.VarianceThresholdSelector()
                .setVarianceThreshold(algorithm.getDouble("varianceThreshold"))
                .setFeaturesCol(featureCol())
                .setOutputCol(featureColComputed());
    }

}