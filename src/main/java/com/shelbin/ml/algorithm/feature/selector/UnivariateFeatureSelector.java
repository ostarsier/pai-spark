package com.shelbin.ml.algorithm.feature.selector;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class UnivariateFeatureSelector extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.UnivariateFeatureSelector()
                .setFeatureType(algorithm.getString("featureType"))
                .setLabelType(algorithm.getString("labelType"))
                .setSelectionMode(algorithm.getString("selectionMode"))
                .setSelectionThreshold(algorithm.getDouble("selectionThreshold"))
                .setFeaturesCol(featureCol())
                .setLabelCol(labelCol())
                .setOutputCol(featureColComputed());
    }

}