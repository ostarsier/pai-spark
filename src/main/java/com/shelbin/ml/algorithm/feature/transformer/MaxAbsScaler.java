package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class MaxAbsScaler extends AbstractEstimator {


    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.MaxAbsScaler()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed());
    }
}