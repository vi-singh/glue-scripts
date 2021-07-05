import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame

def TransformSA(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    sh_df = dfc.select(list(dfc.keys())[0]).toDF()
    sh_dfs  = sh_df.withColumn("sh_struct", explode(col("shipping_addresses")))
    sh_df2 = sh_dfs.select("_sdc_batched_at","_sdc_table_version","_sdc_received_at","_sdc_sequence","sh_struct.*")
    sh_df3 = DynamicFrame.fromDF(sh_df2, glueContext, "unnest4")
    return(DynamicFrameCollection({"CustomTransform4": sh_df3}, glueContext))
def TransformBA(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    oh_df = dfc.select(list(dfc.keys())[0]).toDF()
    oh_dfs  = oh_df.withColumn("oh_struct", explode(array(col("billing_address"))))
    oh_df2 = oh_dfs.select("_sdc_batched_at","_sdc_table_version","_sdc_received_at","_sdc_sequence","external_merchant_id","geoip_country","date_shipped","base_wrapping_cost","shipping_cost_ex_tax","total_inc_tax","wrapping_cost_ex_tax","currency_id","items_total","channel_id","ebay_order_id","shipping_cost_tax","currency_code","is_email_opt_in","handling_cost_tax","shipping_cost_inc_tax","gift_certificate_amount","payment_provider_id","payment_method","wrapping_cost_tax_class_id","handling_cost_tax_class_id","base_shipping_cost","external_id","wrapping_cost_inc_tax","tax_provider_id","id","date_modified","discount_amount","coupon_discount","refunded_amount","total_ex_tax","subtotal_inc_tax","staff_notes","status","cart_id","wrapping_cost_tax","shipping_cost_tax_class_id","status_id","default_currency_id","geoip_country_iso2","customer_id","base_handling_cost","handling_cost_inc_tax","store_credit_amount","custom_status","customer_message","handling_cost_ex_tax","currency_exchange_rate","is_deleted","order_is_digital","ip_address","default_currency_code","shipping_address_count","subtotal_tax","payment_status","items_shipped","external_source","date_created","total_tax","order_source","subtotal_ex_tax","oh_struct.*")
    oh_df3 = DynamicFrame.fromDF(oh_df2, glueContext, "unnest3")
    return(DynamicFrameCollection({"CustomTransform3": oh_df3}, glueContext))
def TransformCoupons(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    c_df = dfc.select(list(dfc.keys())[0]).toDF()
    c_dfs  = c_df.withColumn("c_struct", explode(col("coupons")))
    c_df2 = c_dfs.select("_sdc_batched_at","_sdc_table_version","_sdc_received_at","_sdc_sequence","c_struct.*")
    c_df3 = DynamicFrame.fromDF(c_df2, glueContext, "unnest6")
    return(DynamicFrameCollection({"CustomTransform6": c_df3}, glueContext))
def TransformProducts(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    df = dfc.select(list(dfc.keys())[0]).toDF()
    dfs  = df.withColumn("pr_struct", explode(col("products")))
    df2 = dfs.select("_sdc_batched_at","_sdc_table_version","_sdc_received_at","_sdc_sequence","pr_struct.*")
    df3 = DynamicFrame.fromDF(df2, glueContext, "unnest2")
    return(DynamicFrameCollection({"CustomTransform2": df3}, glueContext))
def TransformAppliedDiscount(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col, explode, flatten, struct,array,from_json
    prd_df = dfc.select(list(dfc.keys())[0]).toDF()
    prd_dfs  = prd_df.withColumn("prd_struct", explode(col("applied_discounts")))
    prd_df2 = prd_dfs.select("_sdc_batched_at","_sdc_table_version","_sdc_received_at","_sdc_sequence","prd_struct.*")
    prd_df3 = DynamicFrame.fromDF(prd_df2, glueContext, "unnest5")
    return(DynamicFrameCollection({"CustomTransform5": prd_df3}, glueContext))

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
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = TransformSA, transformation_ctx = "Transform7"]
## @return: Transform7
## @inputs: [dfc = DataSource0]
Transform7 = TransformSA(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform7.keys())[0], transformation_ctx = "Transform10"]
## @return: Transform10
## @inputs: [dfc = Transform7]
Transform10 = SelectFromCollection.apply(dfc = Transform7, key = list(Transform7.keys())[0], transformation_ctx = "Transform10")
## @type: SelectFields
## @args: [paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "cost_ex_tax", "street_1", "items_total", "country", "country_iso2", "city", "company", "handling_cost_tax", "shipping_method", "handling_cost_tax_class_id", "id", "email", "cost_inc_tax", "order_id", "last_name", "street_2", "base_handling_cost", "handling_cost_inc_tax", "first_name", "phone", "state", "shipping_zone_id", "handling_cost_ex_tax", "cost_tax", "shipping_zone_name", "items_shipped", "zip", "cost_tax_class_id", "base_cost"], transformation_ctx = "Transform8"]
## @return: Transform8
## @inputs: [frame = Transform10]
Transform8 = SelectFields.apply(frame = Transform10, paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "cost_ex_tax", "street_1", "items_total", "country", "country_iso2", "city", "company", "handling_cost_tax", "shipping_method", "handling_cost_tax_class_id", "id", "email", "cost_inc_tax", "order_id", "last_name", "street_2", "base_handling_cost", "handling_cost_inc_tax", "first_name", "phone", "state", "shipping_zone_id", "handling_cost_ex_tax", "cost_tax", "shipping_zone_name", "items_shipped", "zip", "cost_tax_class_id", "base_cost"], transformation_ctx = "Transform8")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderShippingAddresses/", "partitionKeys": []}, transformation_ctx = "DataSink1"]
## @return: DataSink1
## @inputs: [frame = Transform8]
DataSink1 = glueContext.write_dynamic_frame.from_options(frame = Transform8, connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderShippingAddresses/", "partitionKeys": []}, transformation_ctx = "DataSink1")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = TransformBA, transformation_ctx = "Transform6"]
## @return: Transform6
## @inputs: [dfc = DataSource0]
Transform6 = TransformBA(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform6.keys())[0], transformation_ctx = "Transform5"]
## @return: Transform5
## @inputs: [dfc = Transform6]
Transform5 = SelectFromCollection.apply(dfc = Transform6, key = list(Transform6.keys())[0], transformation_ctx = "Transform5")
## @type: SelectFields
## @args: [paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "external_merchant_id", "geoip_country", "date_shipped", "base_wrapping_cost", "shipping_cost_ex_tax", "total_inc_tax", "wrapping_cost_ex_tax", "currency_id", "items_total", "channel_id", "ebay_order_id", "shipping_cost_tax", "currency_code", "is_email_opt_in", "handling_cost_tax", "shipping_cost_inc_tax", "gift_certificate_amount", "payment_provider_id", "payment_method", "wrapping_cost_tax_class_id", "handling_cost_tax_class_id", "base_shipping_cost", "external_id", "wrapping_cost_inc_tax", "tax_provider_id", "id", "date_modified", "discount_amount", "coupon_discount", "refunded_amount", "total_ex_tax", "subtotal_inc_tax", "staff_notes", "status", "cart_id", "wrapping_cost_tax", "shipping_cost_tax_class_id", "status_id", "default_currency_id", "geoip_country_iso2", "customer_id", "base_handling_cost", "handling_cost_inc_tax", "store_credit_amount", "custom_status", "customer_message", "handling_cost_ex_tax", "currency_exchange_rate", "is_deleted", "order_is_digital", "ip_address", "default_currency_code", "shipping_address_count", "subtotal_tax", "payment_status", "items_shipped", "external_source", "date_created", "total_tax", "order_source", "subtotal_ex_tax", "street_1", "country", "country_iso2", "city", "company", "email", "last_name", "street_2", "first_name", "phone", "state", "zip"], transformation_ctx = "Transform11"]
## @return: Transform11
## @inputs: [frame = Transform5]
Transform11 = SelectFields.apply(frame = Transform5, paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "external_merchant_id", "geoip_country", "date_shipped", "base_wrapping_cost", "shipping_cost_ex_tax", "total_inc_tax", "wrapping_cost_ex_tax", "currency_id", "items_total", "channel_id", "ebay_order_id", "shipping_cost_tax", "currency_code", "is_email_opt_in", "handling_cost_tax", "shipping_cost_inc_tax", "gift_certificate_amount", "payment_provider_id", "payment_method", "wrapping_cost_tax_class_id", "handling_cost_tax_class_id", "base_shipping_cost", "external_id", "wrapping_cost_inc_tax", "tax_provider_id", "id", "date_modified", "discount_amount", "coupon_discount", "refunded_amount", "total_ex_tax", "subtotal_inc_tax", "staff_notes", "status", "cart_id", "wrapping_cost_tax", "shipping_cost_tax_class_id", "status_id", "default_currency_id", "geoip_country_iso2", "customer_id", "base_handling_cost", "handling_cost_inc_tax", "store_credit_amount", "custom_status", "customer_message", "handling_cost_ex_tax", "currency_exchange_rate", "is_deleted", "order_is_digital", "ip_address", "default_currency_code", "shipping_address_count", "subtotal_tax", "payment_status", "items_shipped", "external_source", "date_created", "total_tax", "order_source", "subtotal_ex_tax", "street_1", "country", "country_iso2", "city", "company", "email", "last_name", "street_2", "first_name", "phone", "state", "zip"], transformation_ctx = "Transform11")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderHeader/", "partitionKeys": []}, transformation_ctx = "DataSink4"]
## @return: DataSink4
## @inputs: [frame = Transform11]
DataSink4 = glueContext.write_dynamic_frame.from_options(frame = Transform11, connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderHeader/", "partitionKeys": []}, transformation_ctx = "DataSink4")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = TransformCoupons, transformation_ctx = "Transform3"]
## @return: Transform3
## @inputs: [dfc = DataSource0]
Transform3 = TransformCoupons(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform3.keys())[0], transformation_ctx = "Transform12"]
## @return: Transform12
## @inputs: [dfc = Transform3]
Transform12 = SelectFromCollection.apply(dfc = Transform3, key = list(Transform3.keys())[0], transformation_ctx = "Transform12")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderProductDiscounts/", "partitionKeys": []}, transformation_ctx = "DataSink0"]
## @return: DataSink0
## @inputs: [frame = Transform12]
DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform12, connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderProductDiscounts/", "partitionKeys": []}, transformation_ctx = "DataSink0")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"DataSource0": DataSource0}, glueContext), className = TransformProducts, transformation_ctx = "Transform0"]
## @return: Transform0
## @inputs: [dfc = DataSource0]
Transform0 = TransformProducts(glueContext, DynamicFrameCollection({"DataSource0": DataSource0}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform0.keys())[0], transformation_ctx = "Transform2"]
## @return: Transform2
## @inputs: [dfc = Transform0]
Transform2 = SelectFromCollection.apply(dfc = Transform0, key = list(Transform0.keys())[0], transformation_ctx = "Transform2")
## @type: CustomCode
## @args: [dynamicFrameConstruction = DynamicFrameCollection({"Transform2": Transform2}, glueContext), className = TransformAppliedDiscount, transformation_ctx = "Transform4"]
## @return: Transform4
## @inputs: [dfc = Transform2]
Transform4 = TransformAppliedDiscount(glueContext, DynamicFrameCollection({"Transform2": Transform2}, glueContext))
## @type: SelectFromCollection
## @args: [key = list(Transform4.keys())[0], transformation_ctx = "Transform9"]
## @return: Transform9
## @inputs: [dfc = Transform4]
Transform9 = SelectFromCollection.apply(dfc = Transform4, key = list(Transform4.keys())[0], transformation_ctx = "Transform9")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderDiscounts/", "partitionKeys": []}, transformation_ctx = "DataSink3"]
## @return: DataSink3
## @inputs: [frame = Transform9]
DataSink3 = glueContext.write_dynamic_frame.from_options(frame = Transform9, connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderDiscounts/", "partitionKeys": []}, transformation_ctx = "DataSink3")
## @type: SelectFields
## @args: [paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "product_id", "cost_price_inc_tax", "base_wrapping_cost", "total_inc_tax", "wrapping_cost_ex_tax", "width", "sku", "quantity_shipped", "event_date", "return_id", "base_total", "price_ex_tax", "height", "option_set_id", "is_bundled_product", "external_id", "wrapping_cost_inc_tax", "price_inc_tax", "cost_price_tax", "id", "refund_amount", "wrapping_name", "total_ex_tax", "bin_picking_number", "wrapping_message", "name", "quantity", "cost_price_ex_tax", "ebay_transaction_id", "order_id", "wrapping_cost_tax", "is_refunded", "fixed_shipping_cost", "order_address_id", "type", "parent_order_product_id", "depth", "quantity_refunded", "ebay_item_id", "base_price", "event_name", "base_cost_price", "weight", "price_tax", "total_tax"], transformation_ctx = "Transform1"]
## @return: Transform1
## @inputs: [frame = Transform2]
Transform1 = SelectFields.apply(frame = Transform2, paths = ["_sdc_batched_at", "_sdc_table_version", "_sdc_received_at", "_sdc_sequence", "product_id", "cost_price_inc_tax", "base_wrapping_cost", "total_inc_tax", "wrapping_cost_ex_tax", "width", "sku", "quantity_shipped", "event_date", "return_id", "base_total", "price_ex_tax", "height", "option_set_id", "is_bundled_product", "external_id", "wrapping_cost_inc_tax", "price_inc_tax", "cost_price_tax", "id", "refund_amount", "wrapping_name", "total_ex_tax", "bin_picking_number", "wrapping_message", "name", "quantity", "cost_price_ex_tax", "ebay_transaction_id", "order_id", "wrapping_cost_tax", "is_refunded", "fixed_shipping_cost", "order_address_id", "type", "parent_order_product_id", "depth", "quantity_refunded", "ebay_item_id", "base_price", "event_name", "base_cost_price", "weight", "price_tax", "total_tax"], transformation_ctx = "Transform1")
## @type: DataSink
## @args: [connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderDetails/", "partitionKeys": []}, transformation_ctx = "DataSink2"]
## @return: DataSink2
## @inputs: [frame = Transform1]
DataSink2 = glueContext.write_dynamic_frame.from_options(frame = Transform1, connection_type = "s3", format = "json", connection_options = {"path": "s3://trx-data-lake-processed/USbigCommerce/testoutput/OrderDetails/", "partitionKeys": []}, transformation_ctx = "DataSink2")
job.commit()
