package com.shelbin.ml.algorithm.feature.extractors;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class Word2Vec extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.Word2Vec()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed())
                .setVectorSize(algorithm.getInteger("vectorSize"))
                .setMinCount(algorithm.getInteger("minCount"));
    }

}