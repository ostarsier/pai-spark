package com.shelbin.ml.algorithm.feature.extractors;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class CountVectorizerModel extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.feature.CountVectorizer()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed())
                .setVocabSize(algorithm.getInteger("vectorSize"))
                .setMinDF(algorithm.getDouble("minDF"));
    }

}