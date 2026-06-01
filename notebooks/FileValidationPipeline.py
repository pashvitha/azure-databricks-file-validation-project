fileNames = ['Product.csv', 'Order1.csv']

for fileName in fileNames:
    
    fileNameWithoutExt = fileName.split('.')[0]
    
    print("Processing:", fileName)
    print("File without extension:", fileNameWithoutExt)

import pyspark.sql.functions as F
sqlDbName = 'sqldatabase156'
dbUserName = 'Username156'
passwordKey = 'sqlkeyvault156'
stgAccountSASTokenKey = 'sastoken156'
landingFileName =fileName #'Product'  #dbutils.widgets.get('Product')
databricksScopeName ='Secretscop157'
dbServer = 'sqlserver156'
dbServerPortNumber ='1433' 
storageContainer ='input'
storageAccount='storageaccount156'
landingMountPoint ='/mnt

if not any(mount.mountPoint == landingMountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.mount( source = 'wasbs://{}@{}.blob.core.windows.net'.format(storageContainer, storageAccount), mount_point= landingMountPoint, extra_configs ={'fs.azure.sas.{}.{}.blob.core.windows.net'.format(storageContainer,storageAccount):dbutils.secrets.get(scope = databricksScopeName, key= stgAccountSASTokenKey)})
    print('Mounted the storage account successfully')
else:
    print('Storage account already mounted')

#connect to Azure SQL DB
dbPassword = dbutils.secrets.get(scope = databricksScopeName, key= passwordKey)
serverurl = 'jdbc:sqlserver://{}.database.windows.net:{};database={};user={};'.format(dbServer, dbServerPortNumber,sqlDbName,dbUserName)
connectionProperties = {
    'password':dbPassword,
    'driver':'com.microsoft.sqlserver.jdbc.SQLServerDriver'
}
df = spark.read.jdbc(url = serverurl, table = 'dbo.FileDetailsFormat', properties= connectionProperties)
display(df)

# ==========================================================
# 1️⃣ STORAGE AUTHENTICATION (Access Key)
# ==========================================================

spark.conf.set(
    "fs.azure.account.key.storagegdlv.dfs.core.windows.net",
    "<STORAGE_ACCOUNT_KEY>"
)

from pyspark.sql import functions as F

storage_account = "storageaccount156"

# ==========================================================
# 2️⃣ CONNECT TO SQL METADATA (ONLY DATE COLUMNS STORED)
# ==========================================================

sqlDbName = "sqldatabase156"
dbUserName = "Username156"
dbPassword = "PAone@77"
dbServer = "sqlserver156"
dbServerPortNumber = "1433"

serverurl = f"jdbc:sqlserver://{dbServer}.database.windows.net:{dbServerPortNumber};database={sqlDbName};user={dbUserName};"

connectionProperties = {
    "password": dbPassword,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df_meta = spark.read.jdbc(
    url=serverurl,
    table="dbo.FileDetailsFormat",
    properties=connectionProperties
)

# ==========================================================
# 3️⃣ GET ALL FILES FROM LANDING
# ==========================================================

landing_folder  = f"abfss://input@{storage_account}.dfs.core.windows.net/landing/"
staging_folder  = f"abfss://input@{storage_account}.dfs.core.windows.net/staging/"
rejected_folder = f"abfss://input@{storage_account}.dfs.core.windows.net/rejected/"

files = dbutils.fs.ls(landing_folder)

print("Files found:", [f.name for f in files])

# ==========================================================
# 4️⃣ PROCESS EACH FILE
# ==========================================================

for file in files:

    fileName = file.name
    fileNameWithoutExt = fileName.split('.')[0]

    print("==========================================")
    print("Processing:", fileName)

    landing_path  = landing_folder + fileName
    staging_path  = staging_folder + fileName
    rejected_path = rejected_folder + fileName

    df1 = spark.read.csv(landing_path, header=True, inferSchema=True)

    errorFlag = False
    errorMessage = ""

    totalcount = df1.count()
    distinctCount = df1.distinct().count()

    print("Total rows:", totalcount)
    print("Distinct rows:", distinctCount)

    # ======================================================
    # RULE 1 – DUPLICATE CHECK
    # ======================================================

    if distinctCount != totalcount:
        errorFlag = True
        errorMessage += "Duplication Found. "

    # ======================================================
    # RULE 2 – CHECK METADATA EXISTS FOR FILE
    # ======================================================

    expected_df = df_meta.filter(
        F.lower(F.trim(df_meta.FileName)) == fileNameWithoutExt.lower().strip()
    )

    expected_cols = expected_df.select("ColumnName") \
        .rdd.flatMap(lambda x: x).collect()

    actual_cols = df1.columns

    print("Expected date columns:", expected_cols)
    print("Actual file columns:", actual_cols)

    if len(expected_cols) == 0:
        errorFlag = True
        errorMessage += "No metadata found for file. "
    else:
        # --------------------------------------------------
        # RULE 3 – CHECK METADATA COLUMNS EXIST IN FILE
        # --------------------------------------------------

        missing_cols = []

        for col in expected_cols:
            if col not in actual_cols:
                missing_cols.append(col)

        if len(missing_cols) > 0:
            errorFlag = True
            errorMessage += f"Missing columns: {missing_cols}. "

        # --------------------------------------------------
        # RULE 4 – DATE FORMAT VALIDATION
        # --------------------------------------------------

        for r in expected_df.collect():

            colName = r["ColumnName"]
            colFormat = r["ColumnDateFormat"]

            if colName in actual_cols:

                invalidCount = df1.filter(
                    F.to_date(F.col(colName), colFormat).isNull() &
                    F.col(colName).isNotNull()
                ).count()

                if invalidCount > 0:
                    errorFlag = True
                    errorMessage += f"DateFormat incorrect for {colName}. "

    # ======================================================
    # MOVE FILE BASED ON RESULT
    # ======================================================

    if errorFlag:
        dbutils.fs.mv(landing_path, rejected_path)
        print(fileName, "→ REJECTED")
        print("Error:", errorMessage)
    else:
        dbutils.fs.mv(landing_path, staging_path)
        print(fileName, "→ STAGING")
        print("No error")

print("All files processed.")
