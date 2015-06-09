### BASIC CONNECTIVITY TESTING - R STUDIO (Windows Deployment) via RODBC to Amazon Redshift
# Ryan Anderson, June 2015
# Need to install this on Windows first (ODBC COnnection)
# http://docs.aws.amazon.com/redshift/latest/mgmt/install-odbc-driver-windows.html  # Then TEST it from Microsoft 
# For reference - # http://stackoverflow.com/questions/25783571/writing-data-from-an-r-data-set-into-redshift (careful, author had performance issues with this method

library(RODBC)

# what ODBC handles are options to open up a connection channel?  need to pre-configure and TEST OK your AMazon 
odbcDataSources(type = c("all", "user", "system"))  ## show what ODBC Options are on system - should see Amazon

getwd()  #where are we
password <- read.table(file="private.txt", header=FALSE) # where I'm holding pw outside public code , for now
password <- paste(password[1,1],sep="")  # ugly - but masks my password in public code (can also use registry, may update later)

channel <- odbcConnect("AWS_hydrogen_source", uid = "master_user", pwd = password)
channel # works!  if a positive integer, you are connected

odbcGetInfo(channel)
odbcTables(channel, catalog = NULL, schema = NULL, tableName = NULL, tableType = NULL, literal = FALSE)

## LIGHT TEST - THIS IS A BAD METHOD TO CREATE AND LOAD TABLES
df <- data.frame(open=rnorm(50), low=rnorm(50), high=rnorm(50), close=rnorm(50))
#careful about putting everything in lower case - LOWER CASE AWS (no "Low" must be "low")
#http://stackoverflow.com/questions/14882754/how-to-use-sqlsave-function-with-existing-table-in-r
sqlSave(channel,df,"test_table", rownames=F)  # 50 rows about half a minute - 5k Long time
test_data <- sqlQuery(channel,"select * from test_table where close > '0'") # reading is fast. subset
sqlDrop(channel, "test_table", errors = FALSE) # clean up our toys


#### iris
iris
colnames(iris) <- tolower(colnames(iris)) # I think AWS does not like caps in column names
head(iris)
sqlSave(channel,iris,"iris", rownames=F) ## SLOOOOOOW!  SO SLOW! Must be a better way 150 ~1.5 minutes
iris_results <- sqlQuery(channel,"select * from iris where species = 'virginica'") # fast subset. this does work and shows up on AWS Redshift Dashboard
sqlFetch(channel, "iris", max = 5)

## needs work or not sure what these do yet
sqlPrimaryKeys(channel,"iris")
odbcFetchRows(channel, max = 0, buffsize = 1000, nullstring = NA_character_, believeNRows = TRUE)
odbcGetErrMsg(channel)
odbcClearError(channel)

## not working
sqlUpdate(channel, iris_results, "iris")   
sqlColumns(channel, "USArrests")

## clean up our toys
sqlDrop(channel, "iris", errors = FALSE) # clean up our toys
odbcClose(channel)

