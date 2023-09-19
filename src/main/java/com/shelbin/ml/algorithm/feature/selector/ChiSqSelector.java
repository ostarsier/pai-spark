package com.shelbin.ml.algorithm.feature.selector;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

/**
 * 卡方特征选择
 */
@Slf4j
public class ChiSqSelector extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.ChiSqSelector()
                .setNumTopFeatures(algorithm.getInteger("numTopFeatures"))
                .setFeaturesCol(featureCol())
                .setLabelCol(labelCol())
                .setOutputCol(featureColComputed());
    }

}