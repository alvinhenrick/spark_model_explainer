# Spark Model Explainer

This is the library we use and have developed to interperate the Generalized Linear Models and Random Forest.
Currently, the support is for Spark ML GLM and Random Forest.

## Prerequisite
JAVA Version 8  
SBT Version 1.2.8  
SCALA Version 2.11.12  
SPARK Version 2.4.4
XGBOOST Version 1.0.0

Below are the steps to use this library.

1. Download the project and build the jar.
```sbt
   sbt clean assembly
```

2. How to invoke GLM Explainer
```scala
    import com.clarifyhealth.prediction.explainer.GLMExplainTransformer
    val explainTransformer = new GLMExplainTransformer()
    explainTransformer.setCoefficientView("my_coefficients")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setFamily("tweedie")
    explainTransformer.setLinkPower(0.5)
    explainTransformer.setVariancePower(1.0)
    explainTransformer.setNested(true)
    explainTransformer.setCalculateSum(true)
    explainTransformer.setLabel("test")

    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)
```

3. How to invoke RF Explainer
```scala
    import com.clarifyhealth.prediction.explainer.EnsembleTreeExplainTransformer
    val explainTransformer = new EnsembleTreeExplainTransformer()
    explainTransformer.setFeatureImportanceView("my_feature_importance")
    explainTransformer.setPredictionView("my_predictions")
    explainTransformer.setLabel("label")
    explainTransformer.setModelPath(rf_model_path)
    explainTransformer.setDropPathColumn(false)
    val df = spark.emptyDataFrame
    val resultDF = explainTransformer.transform(df)
```

# The Python `spark wrapper` and `scikit-learn` library is here: https://github.com/imanbio/transparency

## Pyspark Example Notebooks
- [PySpark GLM Explain Example](https://github.com/imanbio/transparency/examples/notebooks/spark/pyspark_glm_explain.ipynb)
- [PySpark Random Forest Explain Example](https://github.com/imanbio/transparency/examples/notebooks/spark/pyspark_random_forest_explain.ipynb)

## Authors
* Iman Haji <https://www.linkedin.com/in/imanhaji>
* Imran Qureshi <https://www.linkedin.com/in/imranq2/>
* Alvin Henrick <https://www.linkedin.com/in/alvinhenrick/>

## License
Apache License Version 2.0 


