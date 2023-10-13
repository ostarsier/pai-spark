package com.shelbin.ml.algorithm.base;

import com.alibaba.fastjson2.JSONObject;
import com.shelbin.ml.algorithm.App;
import com.shelbin.ml.algorithm.bean.AnchorConstants;
import com.shelbin.ml.algorithm.bean.ConfigConstants;
import com.shelbin.ml.algorithm.bean.FormatConstants;
import com.shelbin.ml.algorithm.bean.TaskParams;
import com.shelbin.ml.algorithm.utils.ConfigUtils;
import com.google.common.base.Joiner;
import jakarta.xml.bind.JAXBException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.util.HdfsUtils;
import org.dmg.pmml.PMML;
import org.jpmml.model.JAXBUtil;
import org.jpmml.sparkml.PMMLBuilder;

import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.util.Arrays;
import org.apache.spark.sql.functions;

/**
 * 所有spark组件的顶级父类
 * 1、调用execute，负责解析taskParams，构造spark；
 * 2、依次调用方法 before(构建input)、doExecute(构建output和model)、after(写output和model)
 * <p>
 * 通用组件
 * 输入：数据
 * 输出：数据
 */
@Slf4j
public abstract class AbstractComponent {
    protected TaskParams taskParams;
    protected JSONObject algorithm;
    protected JSONObject anchor;
    protected JSONObject algorithmIo;
    protected static final String COMPUTED_SUFFIX = "_computed";
    protected Dataset<Row> inputData;
    protected Dataset<Row> outputData;
    /**
     * 输出的模型
     */
    protected PipelineModel pipelineModel;
    protected SparkSession spark;

    private static String url;
    private static String username;
    private static String password;
    private static String driver;
    private static String dbName;

    static {
        url = ConfigUtils.get(ConfigConstants.CLICKHOUSE_URL);
        dbName = ConfigUtils.get(ConfigConstants.CLICKHOUSE_DB);
        username = ConfigUtils.get(ConfigConstants.CLICKHOUSE_USERNAME);
        password = ConfigUtils.get(ConfigConstants.CLICKHOUSE_PASSWORD);
        driver = ConfigUtils.get(ConfigConstants.CLICKHOUSE_DRIVER);
    }

    /**
     * 反射调用
     *
     * @see App#main(String[])
     */
    public void execute(TaskParams taskParams) throws Exception {
        initParams(taskParams);

        this.spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();

        before();
        doExecute();
        after();

        spark.close();
    }

    protected void before() throws Exception {
        this.inputData = spark.read()
                .format("jdbc")
                .option("createTableOptions", "ENGINE = Log()")
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .option("url", url)
                .option("dbtable", String.format("`%s`.%s", dbName, inputTable()))
                .option("user", username)
                .option("password", password)
                .load();

    }

    protected void after() throws Exception {
        if (anchor.getString(AnchorConstants.OUTPUT_TABLE) != null) {
            write();
        }

        if (anchor.getString(AnchorConstants.OUTPUT_MODEL_PATH) != null) {
            save();
            savePmml();
        }
    }

    public abstract void doExecute() throws Exception;

    protected void write() {
        write(outputData, outputTable());
    }

    protected void write(Dataset<Row> outputData, String outputTable) {
        Dataset<Row>  lowerColumnDataset = columnNameToLowerCase(outputData);

        lowerColumnDataset.write()
                .format("jdbc")
                .option("createTableOptions", "ENGINE = Log()")
                .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
                .option("url", url)
                .option("dbtable", String.format("`%s`.%s", dbName, outputTable))
                .option("user", username)
                .option("password", password)
                .mode(SaveMode.Overwrite)
                .save();
    }

    private static Dataset<Row>  columnNameToLowerCase(Dataset<Row> outputData) {
        Dataset<Row> updatedDataset = outputData;
        for (String columnName : outputData.columns()) {
            updatedDataset = updatedDataset.withColumnRenamed(columnName, columnName.toLowerCase());
        }
        return updatedDataset;
    }

    /**
     * 保存模型
     */
    protected void save() throws IOException {
        pipelineModel.write().overwrite().save(anchor.getString(AnchorConstants.OUTPUT_MODEL_PATH));
    }

    /**
     * 保存模型的PMML格式
     */
    protected void savePmml() throws JAXBException {
        for (String columnName : inputData.columns()) {
            inputData = inputData.withColumn(columnName,
                    functions.col(columnName).cast("double"));
        }
        PMML pmml = new PMMLBuilder(inputData.schema(), pipelineModel).build();
        String pmmlPath = outputModelPath() + ".pmml";
        Configuration conf = new Configuration();
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (StringUtils.isBlank(hadoopConfDir)) {
            log.warn("Unable to save PMML model to HDFS without setting ${HADOOP_CONF_DIR} environment variable");
            return;
        }
        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        JAXBUtil.marshalPMML(pmml, new StreamResult(HdfsUtils.getOutputStream(pmmlPath, conf)));
    }

    private void initParams(TaskParams taskParams) {
        this.taskParams = taskParams;
        this.algorithm = taskParams.getAlgorithm();
        this.anchor = taskParams.getAnchor();
        this.algorithmIo = taskParams.getAlgorithmIo();
    }

    /**
     * @param input  field1,field2,field3
     * @param suffix 后缀，用于PCA、OneHot等Transformer后的新字段
     */
    private String[] cols(String input, String suffix) {
        return Arrays.stream(input.split(","))
                .map(field -> field + suffix)
                .toArray(String[]::new);
    }

    protected String[] cols(String input) {
        return cols(input, "");
    }


    protected String col(String input) {
        return cols(input, "")[0];
    }

    protected String[] featureCols() {
        return cols(algorithmIo.getString(FormatConstants.FEATURES));
    }

    /**
     * 特征列只有一列
     */
    protected String featureCol() {
        return featureCols()[0];
    }

    protected String[] featureColsComputed() {
        return cols(algorithmIo.getString(FormatConstants.FEATURES), COMPUTED_SUFFIX);
    }

    /**
     * 特征列只有一列
     */
    protected String featureColComputed() {
        return featureColsComputed()[0];
    }

    protected String featuresCol() {
        return FormatConstants.FEATURES;
    }

    protected String labelCol() {
        return algorithmIo.getString(FormatConstants.LABEL);
    }

    protected String inputModelPath() {
        return anchor.getString(AnchorConstants.INPUT_MODEL_PATH);
    }

    protected String outputModelPath() {
        return anchor.getString(AnchorConstants.OUTPUT_MODEL_PATH);
    }

    protected String formula() {
        return labelCol() + " ~ " + Joiner.on(" + ").join(featureCols());
    }

    protected String inputTable() {
        return anchor.getString(AnchorConstants.INPUT_TABLE);
    }

    protected String outputTable() {
        return anchor.getString(AnchorConstants.OUTPUT_TABLE);
    }

    protected String outputTable1() {
        return anchor.getString(AnchorConstants.OUTPUT_TABLE_1);
    }

    protected String outputTable2() {
        return anchor.getString(AnchorConstants.OUTPUT_TABLE_2);
    }

}
