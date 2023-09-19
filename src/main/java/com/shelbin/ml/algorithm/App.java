package com.shelbin.ml.algorithm;

import com.shelbin.ml.algorithm.bean.TaskParams;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;

import java.util.Base64;

@Slf4j
public class App {

    public static void main(String[] args) throws Exception {
        String taskParamsJson = new String(Base64.getDecoder().decode(args[0]));
        Gson gson = new Gson();
        TaskParams taskParams = gson.fromJson(taskParamsJson, TaskParams.class);
        log.info("taskParams:{}", taskParams);
        String className = taskParams.getAlgorithmExt().getString("className");
        Class<?> clazz = Class.forName(className);
        Object component = clazz.newInstance();

        MethodUtils.getAccessibleMethod(clazz, "execute", TaskParams.class).invoke(component, taskParams);
    }

}
