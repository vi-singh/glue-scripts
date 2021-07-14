import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.amazonaws.services.glue.log.GlueLogger



import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    // @type: DataSource
    // @args: [database = "trx-data-lake", table_name = "all", transformation_ctx = "datasource0"]
    // @return: datasource0
    
    val sparkSession: SparkSession = glueContext.getSparkSession
    import sparkSession.implicits._
    import java.util.Calendar
    
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    
    
    // show logs
    val logger = new GlueLogger
    logger.info("info message")
    logger.warn("warn message")
    logger.error("error message")
    
    

    val year: Int = Calendar.getInstance().get(Calendar.YEAR)
    val month: Int = Calendar.getInstance().get(Calendar.MONTH) + 1
    val day: Int = Calendar.getInstance().get(Calendar.DATE)-1
    
    
    val df = sparkSession.read.json("s3://trx-data-lake/ushubspot/email_events" + "/%04d".format(year) + "/%02d".format(month)+ "/%02d".format(day) + "/*/*.jsonl")
    // read the processed main data frame
    val mainDF = sparkSession.read.json("s3://trx-data-lake-processed/hubspot-us-email-events-main/")
    
    //Perform processing on the new data.
    
    val eventTypes = List("SENT","DELIVERED","OPEN","CLICK")
    val df2 = df.filter(col("type").isin(eventTypes:_*)).select("emailCampaignId","recipient","type","created")
    
    // Pivot by Event type
    val pivotedDf = df2.
     withColumn("created_timestamp", unix_timestamp($"created".cast("timestamp"))).
     groupBy("emailCampaignId","recipient").
     pivot("type").
     agg(max("created_timestamp").alias("latest_event_timestamp"), 
         min("created_timestamp").alias("earliest_event_timestamp"))
    
    // ReOrder the Columns
    val reorderedDF = pivotedDf.select("emailCampaignId","recipient","SENT_latest_event_timestamp","SENT_earliest_event_timestamp","DELIVERED_latest_event_timestamp",
                 "DELIVERED_earliest_event_timestamp","OPEN_latest_event_timestamp", "OPEN_earliest_event_timestamp","CLICK_latest_event_timestamp","CLICK_earliest_event_timestamp")
                 
                 
    val unionDF = mainDF.union(reorderedDF).distinct()
    
    val blendedDF = unionDF.groupBy("emailCampaignId","recipient").
        agg(
        max("SENT_latest_event_timestamp").alias("SENT_latest_event_timestamp"),
        min("SENT_earliest_event_timestamp").alias("SENT_earliest_event_timestamp"),
        max("DELIVERED_latest_event_timestamp").cast("long").alias("DELIVERED_latest_event_timestamp"),
        min("DELIVERED_earliest_event_timestamp").alias("DELIVERED_earliest_event_timestamp"),
        max("OPEN_latest_event_timestamp").alias("OPEN_latest_event_timestamp"),
        min("OPEN_earliest_event_timestamp").alias("OPEN_earliest_event_timestamp"),
        max("CLICK_latest_event_timestamp").alias("CLICK_latest_event_timestamp"),
        min("CLICK_earliest_event_timestamp").alias("CLICK_earliest_event_timestamp")
        )
        
    blendedDF.write.                       
        mode(SaveMode.Overwrite).
        partitionBy("emailCampaignId").
        format("json").
        option("quote", " ").
        save("s3://trx-data-lake-processed/hubspot-us-email-events-main/")
  
    Job.commit()
  }
}