package com.shelbin.ml.algorithm.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.Transformer;

/**
 * 数据转换组件
 * 输入：数据
 * 输出：数据
 */
@Slf4j
public abstract class AbstractTransformer extends AbstractComponent {

    @Override
    public void doExecute() throws Exception {
        this.outputData = transformer().transform(inputData);
    }

    protected abstract Transformer transformer();

}