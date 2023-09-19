package com.shelbin.ml.algorithm.classification;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

@Slf4j
public class NaiveBayes extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.classification.NaiveBayes()
                .setModelType(algorithm.getString("modelType"))
                .setSmoothing(algorithm.getInteger("smoothing"));
    }

}