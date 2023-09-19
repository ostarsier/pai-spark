package com.shelbin.ml.algorithm.feature.transformer;

import com.shelbin.ml.algorithm.base.AbstractTransformer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Transformer;

@Slf4j
public class IndexToString extends AbstractTransformer {

    @Override
    protected Transformer transformer() {
        return new org.apache.spark.ml.feature.IndexToString()
                .setInputCol(featureCol())
                .setOutputCol(featureColComputed());
    }

}