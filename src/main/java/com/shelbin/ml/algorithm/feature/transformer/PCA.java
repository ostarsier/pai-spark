package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

/**
 * 主成分分析
 */
@Slf4j
public class PCA extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.PCA()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed())
                .setK(algorithm.getInteger("k"));
    }

}