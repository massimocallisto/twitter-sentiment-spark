package it.unicam.da.streaming.sentiment;


import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.stanford_nlp.model.SentimentResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

/**
 * Hello world!
 *
 */
public class AppMongo
{
    public static void main( String[] args )
    {

        System.out.println( "Sentiment to Mongo!" );
        SparkConf sparkConf = new SparkConf()
                .setAppName("JavaTwitterHashTagJoinSentiments")
                .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/local")
                .set("spark.mongodb.output.uri", "mongodb://127.0.0.1/local")
                .setMaster("local[2]");


        // https://github.com/apache/bahir/blob/master/streaming-mqtt/examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

      /*  Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("collection", "local");*
        WriteConfig writeConfig = WriteConfig.create(jssc.sparkContext()).withOptions(writeOverrides);*/


        final JavaReceiverInputDStream<String> stream = MQTTUtils.createStream(
                jssc,
                "tcp://test.mosquitto.org:1883",
                "wago/pfc/cloudconnectivity/example/mqttpublishark237",
                StorageLevel.MEMORY_ONLY_2());


        JavaDStream<String> map = stream.map(s -> {
            String text = s; // TODO: if jason parsing is needed
            Sentiment sentiment = new Sentiment(text);
            /*System.out.println("Sentiment Score: " + result.getSentimentScore());
            System.out.println("Sentiment Type: " + result.getSentimentType());*/
            return sentiment.getSentiment()+";"+text;
        });




        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {

                JavaRDD<SentimentModel> rddModels = stringJavaRDD.map(new MapClass());
                SQLContext sql = new SQLContext(jssc.sparkContext());
                Dataset<Row> sentimentDF = sql.createDataFrame(rddModels, SentimentModel.class);
                sentimentDF.printSchema();
                sentimentDF.show();
                MongoSpark.write(sentimentDF).option("collection", "sentiment").mode("append").save();

            }
        });



        try {

            jssc.start();              // Start the computation
            jssc.awaitTermination();   // Wait for the computation to terminate
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
