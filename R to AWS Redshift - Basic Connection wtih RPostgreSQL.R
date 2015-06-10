### BASIC CONNECTIVITY TESTING - R STUDIO (Windows Deployment) via RODBC to Amazon Redshift
### Exploring RPostgreSQL package (rather than RODBC)

library(RPostgreSQL)  # uses DBI which was created by Hadley Wickham, so it must be good

####################################
##  Step 0 - Let's Get Connected ### 
####################################


# get password (so it doesnt show up in my GitHub Code)
password <- read.table(file="private.txt", header=FALSE) # where I'm holding pw outside public code , for now
password <- paste(password[1,1],sep="")  # ugly - but masks my password in public code (can also use registry, may update later)

### for reference - used section 3 from this post: https://github.com/snowplow/snowplow/wiki/Setting-up-R-to-perform-more-sophisticated-analysis-on-your-Snowplow-data
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, host="hydrogen2.ccngjl5iyb5n.us-east-1.redshift.amazonaws.com", 
                 port="5439",
                 dbname="mydb", 
                 user="master_user", 
                 password=password)

## assumes that IRIS data is ALREADY on redshift in 'iris' table (I did mine in RODBC - see other blog)
the_data <- dbGetQuery(con, "select * from iris where species = 'virginica'")  # works! returns 50 of 150 

#######################################
##  Step 1 - Super Basic Read/Write ### 
#######################################

############  OK - let's see if we can WRITE to Redshift and not have it take too long
# My test data - https://dreamtolearn.com/ryan/data_analytics_viz/18  - iris 2.o :) contains new invented set
# Data:   https://docs.google.com/spreadsheet/ccc?key=0AgjxYjWyopXhdEhMT2JaTlA5REt6TEFIc3VSZ0xMLVE&;usp=sharing  or https://dreamtolearn.com/doc/BL1TCDF3M2V0E4NO1COGFLGSR
iris_200 <- read.csv("Iris Mythica Data Set.csv")
iris_200 <- iris_200[2:6] # get rid of index - 4 measured attributes, plus class 
head(iris_200)

# Package consulted http://cran.r-project.org/web/packages/RPostgreSQL/RPostgreSQL.pdf 
# and https://code.google.com/p/rpostgresql/ helpful and http://docs.aws.amazon.com/redshift/latest/dg/c_redshift-and-postgres-sql.html

##### I had some problems with DBWRITETABLE - # dbWriteTable(con,"newTable",iris_200) # failed - so tried this to try to write to DB
# see http://rpostgresql.googlecode.com/svn/trunk/RPostgreSQL/inst/devTests/demo.r for reference

# Let's create a table for IRIS
dbSendQuery(con, "create table iris_200 (sepallength float,sepalwidth float,petallength float,petalwidth float,species VARCHAR(100));")
dbListFields(con,"iris_200")

iris_200[1,]

## ONE BY ONE insert four rows into the table
dbSendQuery(con, "insert into iris_200 values(5.1,3.5,1.4,0.2,'Iris-setosa');")
dbSendQuery(con, "insert into iris_200 values(5.5,2.5,1.1,0.4,'Iris-setosa');")
dbSendQuery(con, "insert into iris_200 values(5.2,3.3,1.2,0.3,'Iris-setosa');")
dframe <-dbReadTable(con,"iris_200") # ok
dbRemoveTable(con,"iris_200")  # and clean up toys

# works dbSendQuery(con, paste("insert into iris_200 values(5.2,3.3,1.2,0.3,'Iris-setosa');"))

###########################################################
##  Step 2 - Basic Read/Write - how much can we write? ### 
###########################################################
#  concern here this will be slow method, but will try

dbSendQuery(con, "create table iris_200 (sepallength float,sepalwidth float,petallength float,petalwidth float,species VARCHAR(100));")

# build a SIMPLE query ## 
query <- paste("insert into iris_200 values(",
iris_200[1,1],",", 
iris_200[1,2],",",
iris_200[1,3],",", 
iris_200[1,4],",", 
"'",iris_200[1,5],"'",
");",
sep="")

query # look OK?  if so, run below
dbSendQuery(con, query)

## now check results
dframe <-dbReadTable(con,"iris_200") # ok - are you writing what you expect to see?
dbRemoveTable(con,"iris_200")  # and clean up toys

###########################################################
##  Step 3 - Load a 200 row IRiS table into Redshift  #####  
###########################################################
# Works, but as expected is slow. 200 rows of 5 columns about 90 seconds 

for (i in 1:(dim(iris_200)[1]) ) {
query <- paste("insert into iris_200 values(",iris_200[i,1],",",iris_200[i,2],",",
               iris_200[i,3],",",iris_200[i,4],",","'",iris_200[i,5],"'",");",sep="")
print(paste("row",i,"loading data >>  ",query))
dbSendQuery(con, query)
}




data_all <-dbReadTable(con,"iris_200") # ok - are you writing what you expect to see?
data_some <- dbGetQuery(con, "select * from iris_200 where species like '%mythica%'") #
data_some <- dbGetQuery(con, "select * from iris_200 where species like '%mythica%' and sepalwidth > 3.5") #

 
## TO DO - need a faster way to load data.  From the Amazon error messages, may need to load to S3 and then have Redshift or AWS Machine Learning pull from S3

########## END OF CODE ########## END OF CODE ########## END OF CODE

# MISC COMMANDS
dbListTables(con)
dbListFields(con,"newtable")
dbListFields(con,"iris")
data2 <- dbGetQuery(con, "select * from newtable where class = 'Iris-setosa'")
data2 <- dbGetQuery(con, "select * from newtable")
dframe <-dbReadTable(con,"iris") # ok
dframe <-dbReadTable(con,"newtable") # fail - as no data populated

# REDSHIFT - http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-drop-issues.html
# Ran this just in case -  netsh interface ipv4 set subinterface "Local Area Connection" mtu=1500 store=persistent  # http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-drop-issues.html 

#fails - redshift doesnt like writeTableLOADs? Or am I doing something bad?
dbWriteTable(con,"newTable",iris_200) # failed - LOAD source is not supported. (Hint: only S3 or DynamoDB or EMR based load is allowed) # Created the table, but appears did not write data to it
dbWriteTable(con,"myschema.tablex", iris_200, row.names=F)
