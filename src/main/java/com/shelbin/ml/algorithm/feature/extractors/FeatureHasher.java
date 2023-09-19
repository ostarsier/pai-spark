package com.shelbin.ml.algorithm.feature.extractors;

import com.shelbin.ml.algorithm.base.AbstractTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Transformer;

@Slf4j
public class FeatureHasher extends AbstractTransformer {

    @Override
    protected Transformer transformer() {
        return new org.apache.spark.ml.feature.FeatureHasher()
                .setInputCols(featureCols())
                .setOutputCol(featureColComputed());
    }

}