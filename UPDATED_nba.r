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

# TEST on Wins vs Height qplot from dataframe that contains multiple classifiers for team ONLY
mapNBAStats <- function(.,lines)
{
  # key would be for the team only for now
  key <- lines$teamname
  #checkwin <- lines$win
  #win <- ifelse(!is.null(checkwin),checkwin,0)
  values <- cbind(lines$height, lines$weight, lines$minute, lines$fgm, lines$fga, lines$ftm, lines$fta, lines$tpm, lines$tpa, lines$orb, lines$drb, #11 
                 lines$ast, lines$stl, lines$to, lines$blk, lines$pf, lines$pts, lines$teamscore, 1, lines$teamname, lines$win) # 12-20
  keyval(key, values)
  #keyval(unlist(subset(lines,select=c("id"))),1)
}

reduceNBAStats <- function(key, values)
{ 
  occurrences <- sum(values[,19])
  avg_height <- format(round(mean(values[,1], na.rm=TRUE),2),nsmall=1)
  avg_weight <- format(round(mean(values[,2], na.rm=TRUE),2),nsmall=0)
  avg_minute <- format(round(mean(values[,3], na.rm=TRUE),2),nsmall=2)
  avg_teamscore <- format(round(mean(values[,18], na.rm=TRUE),2),nsmall=2)
  avg_pts <-format(round(mean(values[,17], na.rm=TRUE),2),nsmall=2)
  wins <- sum(values[,21])
  teamname <- key
  #wins <- strtoi(wins,)
  values <- cbind(wins, teamname, avg_teamscore, occurrences, avg_height, avg_weight, avg_minute, avg_pts)
  
  keyval(key, values)
}

NBAStats <- mapreduce(input ='/user/biadmin/nba3.csv', input.format =nba.in.format,
                        map = mapNBAStats,
                        reduce = reduceNBAStats
                      )

NBAStats <- as.data.frame(values(from.dfs(NBAStats)))
NBAStats[order(NBAStats$wins),]
View(NBAStats)

#TRAINING Commented out for now 
#data <- createDataPartition(NBAStats$avg_teamscore, p = 0.7, list = FALSE)
#train <-NBAStats[data,] 
#test  <-NBAStats[-data,] 

#qplot(train$avg_height, train$avg_teamscore, color=train$teamname, xlab="height", ylab="avg_teamscore")

#NBAStats$wins NEEDS to be fixed for display
qplot(NBAStats$avg_height, NBAStats$wins, color=NBAStats$teamname, xlab="height", ylab="wins")

