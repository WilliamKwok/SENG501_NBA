###### This part of the code was from the original file
rm(list = ls()) # clear the history from the previous run (i.e. variables)
cat("\014")# Clear Console
closeAllConnections() # close any file connections if any
dev.off()# Clear All Graphs in the plot area

######Set the system parameters and environments variables
hcmd <-system("which hadoop", intern = TRUE)
Sys.setenv(HADOOP_CMD=hcmd)

hstreaming <- system("find /usr -name hadoop-streaming*jar", intern=TRUE)
Sys.setenv(HADOOP_STREAMING= hstreaming[1])

Sys.getenv("HADOOP_CMD")
Sys.getenv("HADOOP_STREAMING")

library(rmr2)
library (rhdfs)
library(lattice)
library(nutshell)
library(igraph)
hdfs.init()

######################### Define the Data format (i.e. according to the available data study the format and adopt it to col classes and names)
col.classes <-
  c(team ="factor", location ="factor",   name ="factor", leag ="factor"
  )
team.in.format <- 
  make.input.format(
    "csv", 
    sep = ",",
    colClasses = col.classes,
    col.names = names(col.classes)
  )

#############Read Tge file using from.dfs
team.hdfs<-from.dfs('/user/biadmin/teams.csv',format=team.in.format) # read the file with a specified format
team_data<-team.hdfs$val