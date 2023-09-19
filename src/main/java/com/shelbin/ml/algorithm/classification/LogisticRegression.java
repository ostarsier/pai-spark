package com.shelbin.ml.algorithm.classification;

import com.shelbin.ml.algorithm.base.AbstractPredictor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Predictor;

@Slf4j
public class LogisticRegression extends AbstractPredictor {

    @Override
    protected Predictor predictor() {
        return new org.apache.spark.ml.classification.LogisticRegression()
                .setElasticNetParam(algorithm.getInteger("elasticNetParam"))
                .setFamily(algorithm.getString("family"))
                .setFitIntercept(algorithm.getBoolean("fitIntercept"))
                .setMaxIter(algorithm.getInteger("maxIter"))
                .setRegParam(algorithm.getInteger("regParam"))
                .setStandardization(algorithm.getBoolean("standardization"))
                .setTol(algorithm.getInteger("tol"));
    }

}