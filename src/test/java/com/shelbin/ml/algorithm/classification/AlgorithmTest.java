package com.shelbin.ml.algorithm.classification;


import com.shelbin.ml.algorithm.App;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Base64;

public class AlgorithmTest {

    @Test
    public void testSplit() throws Exception {
        String splitJson = FileUtils.readFileToString(new File("src/test/resources/Split.json"));
        String encodedString = Base64.getEncoder().encodeToString(splitJson.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testLogisticRegressionClassification() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/LogisticRegressionClassification.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }


    @Test
    public void testRandomForestRegressor() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/RandomForestRegressor.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }



    @Test
    public void ClassificationPrediction() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/ClassificationPrediction.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testRegressionPrediction() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/RegressionPrediction.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }


    @Test
    public void testJdbcSrouce() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/jdbc.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testReplaceMissing() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/ReplaceMissing.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }


    @Test
    public void testSql() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/sql.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testRegressionEvaluator() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/regressionEvaluator.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testMulticlassClassificationEvaluator() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/MulticlassClassificationEvaluator.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testBinaryClassificationEvaluator() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/BinaryClassificationEvaluator.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }

    @Test
    public void testSqlJoinTransformer() throws Exception {
        String json = FileUtils.readFileToString(new File("src/test/resources/sqlJoinTransformer.json"));
        String encodedString = Base64.getEncoder().encodeToString(json.getBytes());
        App.main(new String[]{encodedString});
    }


}