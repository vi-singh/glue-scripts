import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF()
    df2 = df.select("products.product_id","products.sku","currency_code")
    df3 = DynamicFrame.fromDF(df2, glueContext, "unnest")
    return(DynamicFrameCollection({"CustomTransform0": df3}, glueContext))
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    
    df = dfc.select(list(dfc.keys())[0]).toDF()
    dfs  = df.withColumn("pr_struct", explode(col("products")))
    df2 = dfs.select("*","pr_struct.*")
    
    
    df3 = DynamicFrame.fromDF(df2, glueContext, "unnest2")
    return(DynamicFrameCollection({"CustomTransform1": df3}, glueContext))

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://trx-data-lake/usbigcommerce/orders/"], "recurse":True}, transformation_ctx = "DataSource0"]
## @return: DataSource0
## @inputs: []
DataSource0 = glueContext.create_dynamic_frame.from_options(format_options = {"jsonPath":"","multiline":False}, connection_type = "s3", format = "json", connection_options = {"paths": ["s3://trx-data-lake/usbigcommerce/orders/"], "recurse":True}, transformation_ctx = "DataSource0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = MyTransform, transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [dfc = DataSource0]
Transform1 = MyTransform(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform1.keys())[0], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform1]
Transform2 = SelectFromCollection.apply(dfc = Transform1, key = list(Transform1.keys())[0], transformation_ctx = "Transform2")
## @type: Spigot
## @args: [path = "s3://trx-data-lake-processed/USbigCommerce/testoutput/spigot", options = {"topk": 100, "prob": 1.0}, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [frame = Transform2]
Transform3 = Spigot.apply(frame = Transform2, path = "s3://trx-data-lake-processed/USbigCommerce/testoutput/spigot", options = {"topk": 100, "prob": 1.0}, transformation_ctx = "Transform3")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = MyTransform, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource0]
Transform0 = MyTransform(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
job.commit()