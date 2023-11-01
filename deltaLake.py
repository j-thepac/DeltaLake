# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *
import os


# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("trying_delta")\
.config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")\
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
.getOrCreate()

# %%
ip='src/input.csv'
op='op/'
nK=["id"]
hashCol="SHA256"
sKey="sK"

# %%
df = (spark
        .read
        .format('csv')
        .option('header', 'true')
        .option('inferSchema', 'true')
        .load(ip))

df.show()

# %%
def handleIntegerType(df:DataFrame,cols:list[str])->DataFrame:
    return df.na.fill(0,cols)

def handleStringType(df:DataFrame,cols:list[str])->DataFrame:
    df=df.na.fill("NA",cols)
    for c in cols:
        df=df.withColumn(c,trim(df[c]))
    return df
        
def cleanDF(df:DataFrame)->DataFrame:
    d={}
    for field in df.schema:
        fieldType=str(field.dataType).replace("()","")
        if((fieldType in d.keys()) == False):
            d[fieldType]=[field.name]
        else:
            d[fieldType].append(field.name)

    for key,cols  in d.items():
        if key=='IntegerType': df=df.transform(handleIntegerType,cols);
        elif key=='StringType': df=df.transform(handleStringType,cols);
    return df

# %%
df=df.transform(cleanDF)
df.show()

# %%
def addHash(df:DataFrame,nK:list[str])->DataFrame:
    df= df.withColumn(hashCol,sha2(concat_ws("_",*nK),256))
    return df

def addSkey(df:DataFrame)->DataFrame:
    return df.withColumn(sKey,row_number().over(Window.orderBy(hashCol)))

def getDeltaDF():
    if os.path.exists(op):
        deltaDF=DeltaTable.forPath(spark,op)
        return deltaDF
    return 0

def fullLoad(df:DataFrame,nK:list[str]):
   df=df.transform(addHash,nK)
   return df.transform(addSkey) 

def deltaLoad(srdDF:DataFrame,deltaDF:DeltaTable,nK:list[str]): 
    tempDeltaDF=deltaDF.toDF().select(hashCol,sKey)
    maxKey=tempDeltaDF.select(max(sKey)).first()[f"max({sKey})"]

    srcDF=srdDF.transform(addHash,nK)
    fullDF=srcDF.join(tempDeltaDF,[hashCol],"full")
    newRecordsDF=fullDF.filter("sk is null").transform(addSkey).withColumn(sKey,maxKey+col(sKey)+1)
    fullDF=fullDF.filter("sk is not null").union(newRecordsDF)
    # fullDF.show()
    return fullDF
    
    # if(newRecordsDF.count()==0):print(f" No records with new naturalKeys {nK}, Checking for Updated Records  ")
    # else:
    #     newRecordsDF=newRecordsDF.transform(addSkey)
    #     maxKey=deltaDF.toDF().select(max(sKey)).first()[f"max({sKey})"]
    #     newRecordsDF=newRecordsDF.withColumn(sKey,df.sKey+maxKey+1)

    

# %%
deltaDF=getDeltaDF()

if deltaDF==0:
    resDF=fullLoad(df,nK)
    resDF.write.format("delta").save(op)
else:
    resDF=deltaLoad(df,deltaDF,nK)


# %%
deltaLoad(df,deltaDF,nK)


