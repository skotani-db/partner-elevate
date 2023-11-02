# Databricks notebook source
class StreamFactory:
    def __init__(self, target_dir, load_batch, max_batch):
        self.target_dir = target_dir
        self.load_batch = load_batch
        self.max_batch = max_batch
        self.batch = 1
        
        
    def load(self, continuous=False):
        import time
        start = dbgems.clock_start()
        if self.batch > self.max_batch:
            print("Data source exhausted", end="...")
            total = 0                
        elif continuous == True:
            print(f"Loading all batches to the stream", end="...")
            end_batch = self.max_batch
        else:
            print(f"Loading batch #{self.batch} to the stream", end="...")
            end_batch = self.batch
            
        total = self.load_batch(self.target_dir, self.batch, end_batch)
        self.batch = end_batch + 1
        print(f"({int(time.time())-start} seconds, {total:,} records)")

# COMMAND ----------

# registered users batch load function

def load_user_reg_batch(target_dir, batch_start, batch_end):
    from pyspark.sql import functions as F

    df = (spark.read
          .format("json")
          .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
          .load(f"{DA.paths.datasets}/user-reg")
          .withColumn("date", F.col("registration_timestamp").cast("timestamp").cast("date"))
          .withColumn("batch", F.when(F.col("date") < "2019-12-01", F.lit(0)).otherwise(F.dayofmonth(F.col("date"))))
          .filter(f"batch >= {batch_start}")
          .filter(f"batch <= {batch_end}")          
          .drop("date", "batch")
          .cache())

    df.write.mode("append").format("json").save(target_dir)
    return df.count()        


# users cdc batch load function

def load_cdc_batch(target_dir, batch_start, batch_end):
    source_dir = f"{DA.paths.datasets}/pii/raw"
    df = (spark.read
      .load(source_dir)
      .filter(f"batch >= {batch_start}")
      .filter(f"batch <= {batch_end}")
    )   
    df.write.mode("append").format("json").save(target_dir)
    return df.count()


# bronze daily batch load function

def load_daily_batch(target_dir, batch_start, batch_end):
    from pyspark.sql import functions as F
    source_dir = f"{DA.paths.datasets}/bronze"
    df = (spark.read
      .load(source_dir)
      .withColumn("day", 
        F.when(F.col("date") <= '2019-12-01', 1)
        .otherwise(F.dayofmonth("date")))
      .filter(F.col("day") >= batch_start)
      .filter(F.col("day") <= batch_end)
      .drop("date", "week_part", "day")  
    )
    df.write.mode("append").format("json").save(target_dir)
    return df.count()    

    
    
# init data stream factory for registered users, users cdc, bronze daily
    
DA.user_reg_stream = StreamFactory(
  target_dir=f"{DA.paths.stream_source}/pii/raw_user_reg",
  max_batch=16,
  load_batch=load_user_reg_batch
)
  
DA.cdc_stream_factory = StreamFactory(
  target_dir=f"{DA.paths.stream_source}/cdc",
  max_batch=3,
  load_batch=load_cdc_batch
)  

DA.daily_stream_factory = StreamFactory(
  target_dir=f"{DA.paths.stream_source}/daily",
  max_batch=16,
  load_batch=load_daily_batch
)         

None # Suppressing Output

