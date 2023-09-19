package com.shelbin.ml.algorithm.pretreatment;

import com.shelbin.ml.algorithm.base.AbstractComponent;

/**
 * 随机采样
 */
public class RandomSampling extends AbstractComponent {

    @Override
    public void doExecute() {

        /**
         * 采样比例: 取值为浮点数，范围(0,1)
         */
        double fraction = algorithm.getDouble("fraction");
        /**
         * 随机种子
         */
        long seed = algorithm.getLong("seed");
        /**
         * 放回采样
         */
        boolean withReplacement = algorithm.getBoolean("withReplacement");

        this.outputData = inputData.sample(withReplacement, fraction, seed);

    }

}
