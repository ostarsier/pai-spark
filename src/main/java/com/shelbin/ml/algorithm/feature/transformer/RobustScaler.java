package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class RobustScaler extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.RobustScaler()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed())
                .setWithScaling(algorithm.getBoolean("withScaling"))
                .setWithCentering(algorithm.getBoolean("withCentering"))
                .setLower(algorithm.getDouble("lower"))
                .setUpper(algorithm.getDouble("upper"));
    }

}