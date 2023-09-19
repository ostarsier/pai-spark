package com.shelbin.ml.algorithm.association;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class FPGrowth extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.fpm.FPGrowth()
                .setMinConfidence(algorithm.getInteger("minConfidence"))
                .setMinSupport(algorithm.getInteger("minSupport"));
    }

}