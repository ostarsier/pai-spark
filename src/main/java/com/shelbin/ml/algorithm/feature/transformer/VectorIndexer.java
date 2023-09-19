package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class VectorIndexer extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.VectorIndexer()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed())
                .setMaxCategories(algorithm.getInteger("maxCategories"));
    }

}