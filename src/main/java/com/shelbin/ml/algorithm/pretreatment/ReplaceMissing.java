package com.shelbin.ml.algorithm.pretreatment;

import com.shelbin.ml.algorithm.base.AbstractEstimator;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.feature.Imputer;

/**
 * 缺失值填充Imputer
 */
public class ReplaceMissing extends AbstractEstimator {

    @Override
    protected Estimator estimator() {
        String strategy = algorithm.getString("strategy");
        return new Imputer()
                .setInputCols(featureCols())
                .setOutputCols(featureColsComputed())
                .setStrategy(strategy);
    }

}