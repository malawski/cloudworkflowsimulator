library(DBI)
library(RSQLite)
library(gmp)




############################################################################################################################
# Start
############################################################################################################################




driver<-dbDriver("SQLite")
connect<-dbConnect(driver, dbname = "pareto-dilated3.sqlite")
dbListTables(connect)

# Plot planning times

q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment WHERE NOT algorithmName LIKE '%DS'")
algorithms <- fetch(q)$algorithm
n_algorithms = length(algorithms)

pdf(file=paste("time-planning", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT times.planningWallTime/1000000000.0 as times FROM times JOIN experiment ON times.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$times	
	hist(times, xlab="planning time in seconds", main=paste(algorithmName), col="lightblue")
	
}
dev.off()

# plot simulation times

q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment")
algorithms <- fetch(q)$algorithm
n_algorithms = length(algorithms)

pdf(file=paste("time-simulation", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT times.simulationWallTime/1000000000.0 as times FROM times JOIN experiment ON times.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$times	
	hist(times, xlab="simulation time in seconds", main=paste(algorithmName), col="lightblue")
	
}
dev.off()


# plot init times

q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment")
algorithms <- fetch(q)$algorithm
n_algorithms = length(algorithms)

pdf(file=paste("time-init", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT times.initWallTime/1000000000.0 as times FROM times JOIN experiment ON times.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$times	
	hist(times, xlab="init time in seconds", main=paste(algorithmName), col="lightblue")
	
}
dev.off()

# plot total times

q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment")
algorithms <- fetch(q)$algorithm
n_algorithms = length(algorithms)

pdf(file=paste("time-total", ".pdf", sep=""), height=3, width=3*n_algorithms, bg="white")
par(mfrow=c(1,n_algorithms))

for(alg in 1:n_algorithms){
	algorithmName = algorithms[[alg]]
	q <- dbSendQuery(connect, statement = sprintf(
					"SELECT (times.initWallTime+times.planningWallTime+times.simulationWallTime)/1000000000.0 as times FROM times JOIN experiment ON times.experiment_id=experiment.id WHERE experiment.algorithmName = '%s'", 
					algorithmName))
	
	times <- fetch(q,-1)$times	
	hist(times, xlab="total time in seconds", main=paste(algorithmName), col="lightblue")
	
}
dev.off()


q <- dbSendQuery(connect, statement = sprintf(
				"SELECT experiment.algorithmName AS algorithm, SUM(times.initWallTime/1000000000.0/3600) AS init, SUM(times.planningWallTime/1000000000.0/3600) AS planning, SUM(times.simulationWallTime/1000000000.0/3600) AS simulation FROM times JOIN experiment ON times.experiment_id=experiment.id GROUP BY algorithmName", 
				algorithmName))

times <- fetch(q,-1)

print(times)

dbDisconnect(connect)









