package com.shelbin.ml.algorithm.cluster;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Estimator;

@Slf4j
public class KMeans extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        return new org.apache.spark.ml.clustering.KMeans()
                .setK(algorithm.getInteger("k"))
                .setMaxIter(algorithm.getInteger("maxIter"))
                .setTol(algorithm.getDouble("tol"))
                .setFeaturesCol(featureCol());
    }


}