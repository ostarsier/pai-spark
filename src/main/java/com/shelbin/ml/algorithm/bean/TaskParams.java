package com.shelbin.ml.algorithm.bean;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class TaskParams {

    private JSONObject algorithmIo;
    private JSONObject algorithm;
    private JSONObject resource;
    private JSONObject anchor;

    private JSONObject algorithmExt;

}
