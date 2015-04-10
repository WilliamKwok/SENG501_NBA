# NBA bball

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
library(ISLR)
library(ggplot2)
library(caret)
library(Hmisc)
library(kernlab)
library(rmr2)
library (rhdfs)
library(lattice)
library(nutshell)
library(igraph)
hdfs.init()

nba.col.classes <-
  c(id="factor", name="factor",  startyear ="integer",
    endyear ="integer", position ="factor", college ="factor",  height="double", 
    weight="integer",  birthday="factor",  city="factor",state="factor", 
    country="factor", hof="factor", draftyear="factor", draftteam="factor", 
    season="factor", teamname="factor",teamscore="factor", month="factor", 
    day="integer",otherteam="factor", otherteamscore="factor", minute="integer",
    fgm="integer", fga="integer", ftm="integer", fta="integer", 
    tpm="integer", tpa="integer", orb="integer", drb="integer",
    tot="integer", ast="integer", stl="integer", to="integer", 
    blk="integer",  pf="integer", pts="integer", attendance="factor")

nba.in.format <-
  make.input.format(
    "csv",
    sep = ",",
    colClasses = nba.col.classes,
    col.names = names(nba.col.classes)
  )

#coachesCareer
nba.hdfs<-from.dfs('/user/group7/project/nba3.csv',format=nba.in.format)
nba_data<-nba.hdfs$val

Unique.nba.id.mapreduce <- mapreduce(input='/user/group7/project/nba3.csv', input.format=nba.in.format,
                                     map = function(k,v) {keyval(unlist(subset(v,select = 
                                     c("id"))),1)},
                                     reduce =function(k,v){keyval(k,sum(v))},
                                     combine=TRUE)
Unique.nba.id.mapreduce.Val <-(from.dfs(Unique.nba.id.mapreduce))
names(Unique.nba.id.mapreduce.Val)[1] <-"id"
names(Unique.nba.id.mapreduce.Val)[2] <-"count"
View(Unique.nba.id.mapreduce.Val)