package org.wbs;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.LongAccumulator;
import java.util.*;
import static org.apache.spark.sql.types.DataTypes.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;



/**
 * Created by cristu on 29/08/16.
 */
public class LinReg {

    public static SparkConf conf = null;
    public static JavaSparkContext sc = null;
    public static SparkSession spark= null;

    public void start(){
        conf = new SparkConf().setAppName("Predictions").setMaster("local[4]");
        sc = new JavaSparkContext(conf);
        spark = SparkSession
                .builder()
                .appName("Predictions-SQL")
                .master("local[4]")
                .getOrCreate();
        sc.setLogLevel("ERROR");
        spark.sparkContext().setLogLevel("ERROR");
    }
    public void stop(){
        spark.stop();
    }

    public static void makePrediction(String filename, List<String> dataDb) {

        try{
            //***********************Get JSON from DB??????*************************

            List<String> dataJson = new ArrayList<String>();
            dataJson.add(filename);
            JavaRDD<String> dataSpark = sc.parallelize(dataJson);
            Dataset<Row> df = spark.read().json(dataSpark);


            df.cache();
            Dataset<Row> dfrows = df.select("ID", "USER_ID", "POSITIVE", "NEUTRAL", "NEGATIVE")
                    .filter(new FilterFunction<Row>() {
                        @Override
                        public boolean call(Row row) throws Exception {
                            return !row.isNullAt(0);
                        }
                    });

            dfrows.cache();

            LongAccumulator accum = sc.sc().longAccumulator();

            List<Row> listdf = dfrows.collectAsList();
            List<Row> mylist = new ArrayList<Row>();

            //Number of comments for make the regression ******************** It could be better getting comments from last day ******************* Now half comments

            int numComment = listdf.size();

            for (int i = numComment; i < listdf.size(); i++) {
                mylist.add(RowFactory.create(Vectors.dense(new Double(i)), new Integer(listdf.get(i).getString(1)), new Double(listdf.get(i).getString(2)), new Double(listdf.get(i).getString(3)), new Double(listdf.get(i).getString(4))));
            }
            StructType schema = new StructType(new StructField[]{
                    createStructField("features", new VectorUDT(), false, Metadata.empty()),
                    createStructField("user_id", IntegerType, false),
                    createStructField("positive", DoubleType, false),
                    createStructField("neutral", DoubleType, false),
                    createStructField("negative", DoubleType, false)
            });
            Dataset<Row> data = spark.createDataFrame(mylist, schema);

            data.cache();

            Dataset<Row>[] splits = data.randomSplit(new double[]{0.70, 0.30});
            Dataset<Row> trainingData = splits[0];
            Dataset<Row> testDatab = splits[1];

            int user_id = data.collectAsList().get(0).getInt(1);

            long numData = dfrows.count();

            List<Row> testList = testDatab.collectAsList();
            List<Row> myPredictList = new ArrayList<Row>();
            myPredictList.addAll(testList);

            //*********************** Add new comments for make predictions ***************************
            int commentsToAdd = 1;

            for (int i = 0; i < commentsToAdd; i++) {
                myPredictList.add(RowFactory.create(Vectors.dense(new Double(numData + i )), user_id, 0.0, 0.0, 0.0));
            }
            Dataset<Row> predictData = spark.createDataFrame(myPredictList, schema);


            GeneralizedLinearRegression glr = new GeneralizedLinearRegression()
                    .setFamily("gaussian")
                    .setLink("identity")
                    .setMaxIter(100)
                    .setRegParam(0.05);

            ParamMap paramMapPos = new ParamMap()
                    .put(glr.labelCol().w("positive"))
                    .put(glr.predictionCol().w("predictionPos"));
            ParamMap paramMapNeu = new ParamMap()
                    .put(glr.labelCol().w("neutral"))
                    .put(glr.predictionCol().w("predictionNeu"));
            ParamMap paramMapNeg = new ParamMap()
                    .put(glr.labelCol().w("negative"))
                    .put(glr.predictionCol().w("predictionNeg"));

            //        // Fit the model
            GeneralizedLinearRegressionModel modelPos = glr.fit(trainingData, paramMapPos);
            GeneralizedLinearRegressionModel modelNeu = glr.fit(trainingData, paramMapNeu);
            GeneralizedLinearRegressionModel modelNeg = glr.fit(trainingData, paramMapNeg);

            Dataset<Row> resultsPos = modelPos.transform(predictData);
            Dataset<Row> resultsNeu = modelNeu.transform(predictData);
            Dataset<Row> resultsNeg = modelNeg.transform(predictData);
            //        // Print the coefficients and intercept for generalized linear regression model
//        System.out.println("Coefficients: " + model.coefficients());
//        System.out.println("Intercept: " + model.intercept());
////
////        // Summarize the model over the training set and print out some metrics
//        GeneralizedLinearRegressionTrainingSummary summary = model.summary();
//        System.out.println("Coefficient Standard Errors: "
//                + Arrays.toString(summary.coefficientStandardErrors()));
//        System.out.println("T Values: " + Arrays.toString(summary.tValues()));
//        System.out.println("P Values: " + Arrays.toString(summary.pValues()));
//        System.out.println("Dispersion: " + summary.dispersion());
//        System.out.println("Null Deviance: " + summary.nullDeviance());
//        System.out.println("Residual Degree Of Freedom Null: " + summary.residualDegreeOfFreedomNull());
//        System.out.println("Deviance: " + summary.deviance());
//        System.out.println("Residual Degree Of Freedom: " + summary.residualDegreeOfFreedom());
//        System.out.println("AIC: " + summary.aic());
//        System.out.println("Deviance Residuals: ");
//        summary.residuals().show();

            List<Row> listPos = resultsPos.collectAsList();
            List<Row> listNeu = resultsNeu.collectAsList();
            List<Row> listNeg = resultsNeg.collectAsList();


            List<Row> resultsList = new ArrayList<Row>();
            for (int i = 0; i < listPos.size(); i++) {
                double sumPred = Precision.round(listNeg.get(i).getDouble(5) + listNeu.get(i).getDouble(5) + listPos.get(i).getDouble(5), 3);
                resultsList.add(RowFactory.create(i, listPos.get(i).get(1), listPos.get(i).get(2), listPos.get(i).get(3), listPos.get(i).get(4), listPos.get(i).get(5), listNeu.get(i).get(5), listNeg.get(i).get(5), sumPred));
            }
            StructType schemaResults = new StructType(new StructField[]{
                    createStructField("comment_id_spark", IntegerType, false),
                    createStructField("user_id", IntegerType, false),
                    createStructField("positive", DoubleType, false),
                    createStructField("neutral", DoubleType, false),
                    createStructField("negative", DoubleType, false),
                    createStructField("predPositive", DoubleType, false),
                    createStructField("predNeutral", DoubleType, false),
                    createStructField("predNegative", DoubleType, false),
                    createStructField("SumPred", DoubleType, false)
            });
            Dataset<Row> results = spark.createDataFrame(resultsList, schemaResults);
            Dataset<Row> resWrite = results.orderBy(results.col("comment_id_spark").asc()).filter(new FilterFunction<Row>() {
                @Override
                public boolean call(Row row) throws Exception {
                    return row.getDouble(2)==0.0 && row.getDouble(3)==0.0 && row.getDouble(4)==0.0;
                }
            });
            Dataset<Row> pred = resWrite.select("user_id", "predPositive", "predNeutral", "predNegative");


            Properties prop = new Properties();
            prop.setProperty("user", dataDb.get(2));
            prop.setProperty("password", dataDb.get(3));
            prop.setProperty("driver", "com.mysql.jdbc.Driver");

            pred.write().mode("overwrite").jdbc(dataDb.get(0), dataDb.get(1), prop);
        } catch (Exception e){
            System.out.println("Linear Regression exception: " + e.getMessage());
        }



    }
}
