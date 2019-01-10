# STRUCTURE
# 项目结构 

* Projection 【内部是各个 module】
    * Azkaban
    * Hive  
            * 模块说明
    * Item (项目案例)  
         * 模块说明项目介绍
         * MR模块 mapreduce 项目 
    * Spark
        * spark-sql  【各个子项目根据数据源不同再作细分】
            * 读写 Hive
            * 读写 MongoDB 
        * spark-core
        * spark-streaming
            * 基础数据源 
            * 高级数据源 
            * 自定义数据源 
        * 模块说明  
    * Shell
        * 模块说明
    * Strom  
    
    * MapReduce
    
    * Zookeeper
    
    




## 依赖问题
通用模块定义在外部 pom.xml 避免依赖冲突 
创建新模块时请优先使用父项目依赖

