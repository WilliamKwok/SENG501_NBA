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
    season="factor", teamname="factor",teamscore="integer", month="factor", 
    day="integer",otherteam="factor", otherteamscore="integer", minute="integer",
    fgm="integer", fga="integer", ftm="integer", fta="integer", 
    tpm="integer", tpa="integer", orb="integer", drb="integer",
    tot="integer", ast="integer", stl="integer", to="integer", 
    blk="integer",  pf="integer", pts="integer", attendance="factor",win="integer")

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
###mapreduce for average
map.averagehgt <- function(.,lines){
  key <-lines[17]
  values<-lines[,7]
  keyval(key,values)
}
reduce.averagehgt <-function (key,values){
  average_height <-mean(values,na.rm=TRUE)
  value<-cbind(key,average_height)
  keyval(key,value)
}
Unique.nba.averagehgt.mapreduce <-mapreduce(input='/user/group7/project/nba3.csv',input.format=nba.in.format, 
                                    map=map.averagehgt,
                                    reduce=reduce.averagehgt,
                                    )
Unique.nba.averagehgt.Val <-values(from.dfs(Unique.nba.averagehgt.mapreduce))
View(Unique.nba.averagehgt.Val)
height<-as.data.frame(Unique.nba.averagehgt.Val)
subsetheight<-subset(height,select=c("teamname","average_height"))
qplot(teamname,average_height,data=subsetheight,xlab="team", ylab="height")
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

NBAStats <- mapreduce(input ='/user/group7/project/nba3.csv', input.format =nba.in.format,
                      map = mapNBAStats,
                      reduce = reduceNBAStats,
)
#Unique.nba.averagehgt.Val <-values(from.dfs(Unique.nba.averagehgt.mapreduce))
NBAStats <- as.data.frame(values(from.dfs(NBAStats)))
#NBAStats[order(NBAStats$wins),]
#View(NBAStats)
#data<-createDataPartition(NBAStats$avg_teamscore,p=0.7, list =FALSE)
#train<-NBAStats[data,]
#test<-NBAStats[-data,]

p<-qplot(avg_height,wins, data = NBAStats,color= NBAStats$teamname, xlab="height",
      ylab="avg_teamscore")
coef(lm(wins ~ avg_height, data=NBAStats))
p+geom_abline(intercept = 8, slope = 1.8)
#lines(lowess(NBAStats$avg_height,NBAStats$wins),col="blue")
qplot(teamname, avg_teamscore, data=NBAStats, xlab="teamname",ylab="teamscore")
count = table(NBAStats$teamname)
barplot(count, xlab="teamname")

#TRAINING Commented out for now 
#data <- createDataPartition(NBAStats$avg_teamscore, p = 0.7, list = FALSE)
#train <-NBAStats[data,] 
#test  <-NBAStats[-data,] 


attach(subsetheight)
plot(subsetheight, xlab=teamname,ylab="average_height")
abline(lm(subsetheight),col="red")
lines(lowess(subsetheight),col="blue")