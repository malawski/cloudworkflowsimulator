library(DBI)
library(RSQLite)
library(gmp)
library(plotrix)




############################################################################################################################
# Start
############################################################################################################################




driver<-dbDriver("SQLite")
connect<-dbConnect(driver, dbname = "pareto-nodelays-logs.sqlite")
dbListTables(connect)



q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment")
algorithms <- fetch(q)$algorithm
n_algorithms = length(algorithms)


# Plot start times

pdf(file=paste("vm-start", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT vm.startTime/3600.0 as startTime FROM vm JOIN experiment ON vm.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$startTime	
	hist(times, xlab="vm start time in hours", main=paste(algorithmName), col="lightblue")
	
}
dev.off()

# Plot finish times

pdf(file=paste("vm-finish", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT vm.finishTime/experiment.deadline as finishTime FROM vm JOIN experiment ON vm.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$finishTime	
	hist(times, xlab="vm finish time / deadline", main=paste(algorithmName), col="lightblue")
	
}
dev.off()

# Plot differences between deadline and finish times

pdf(file=paste("vm-deadline-finish", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT (experiment.deadline - vm.finishTime)/3600 as finishTime FROM vm JOIN experiment ON vm.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$finishTime	
	hist(times, xlab="vm  deadline - finish time", main=paste(algorithmName), col="lightblue")
	
}
dev.off()


# Plot Gantt chart

algorithmName = "WADPDS"
q <- dbSendQuery(connect, statement = sprintf("SELECT id FROM experiment WHERE algorithmName='%s' ORDER BY deadline",algorithmName))
experiments <- fetch(q,-1)$id
n_experiments = length(experiments)

pdf(file=paste("vm-gantt", ".pdf", sep=""), height=10, width=10, bg="white")
#oldpar<-panes(matrix(1:100,nrow=10,byrow=TRUE))


Ymd.format <- "%Y-%m-%d %H:%M:%S"
Ymd <- function(x){ as.POSIXct(strptime(x, format=Ymd.format))}

for(experiment in experiments){
	q <- dbSendQuery(connect, statement = sprintf("SELECT deadline FROM experiment WHERE id='%s'", experiment))
	q_data <- fetch(q,-1)
	deadline = q_data$deadline[1]/3600
	
	q <- dbSendQuery(connect, statement = sprintf(
				"SELECT vmid, datetime(startTime,'unixepoch','+1 seconds') AS startTime, datetime(finishTime,'unixepoch') AS finishTime FROM vm WHERE experiment_id='%s' ORDER BY vmid", 
				experiment))
	q_data <- fetch(q,-1)

	starts     =Ymd(q_data$startTime)
	ends       =Ymd(q_data$finishTime)
	gantt.info <- list(
		labels     =q_data$vmid,
		starts = starts,
		ends = ends)

	gantt.chart(gantt.info,main=paste("VM Gantt chart, deadline = ",deadline), taskcolors="lightgray",format=Ymd.format,vgrid.format="%d %H:%M:%S")
#	tab.title("Boxplot of y",tab.col="#88dd88")
}

dev.off()
dbDisconnect(connect)