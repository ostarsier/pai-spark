
阿里PAI运行DAG时的算法组件：Spark实现
- 核心思想： 
  - DAG构建并运行Spark可以用DolphinScheduler 
  - 输入输出表用ClickHouse，Spark转成DataSet<Row>
  - 模型输出放到HDFS
  - 核心逻辑：AbstractComponent
  - 所有的算法组件根据输入输出的类型和个数划分为：AbstractEstimator、AbstractPredictor、AbstractTransformer

- 测试类：AlgorithmTest
- 入口类：App
- 核心父类：AbstractComponent
- 数据源组件：JdbcSource
- 随机森林回归组件：RandomForestRegressor
- 模型预测组件：Prediction
- 模型评估组件：RegressionEvaluator