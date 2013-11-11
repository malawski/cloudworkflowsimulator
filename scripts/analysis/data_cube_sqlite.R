library(DBI)
library(RSQLite)
library(cwhmisc)



############################################################################################################################
# Start
############################################################################################################################



db_filename = "fractions-nodelays-50-2.sqlite"
driver<-dbDriver("SQLite")
connect<-dbConnect(driver, dbname = db_filename)
#connect<-dbConnect(driver, dbname = "pareto-nodelays-all.sqlite")
#connect<-dbConnect(driver, dbname = "test3.sqlite")
dbListTables(connect)

q <- dbSendQuery(connect, statement = "PRAGMA synchronous = OFF")
fetch(q)
q <- dbSendQuery(connect, statement = "PRAGMA journal_mode = MEMORY")
fetch(q)
#q <- dbSendQuery(connect, statement = "SELECT load_extension('/home/malawski/projects/sqlite/libsqlitefunctions.so')")
#fetch(q)

q <- dbSendQuery(connect, statement = "SELECT COUNT(DISTINCT runID) AS n_runIds FROM experiment ")
d <- fetch(q)
n_runIds=d$n_runIds

q <- dbSendQuery(connect, statement = "SELECT max(finished) AS n_DAGs FROM experiment")
d <- fetch(q)
n_DAGs=d$n_DAGs
max_priority = n_DAGs-1

# n_runIds=1 #use this to select only the first runId

print(n_runIds)

q <- dbSendQuery(connect, statement = "SELECT DISTINCT application FROM experiment ORDER BY application")
applications <- fetch(q)$application
print(applications)
n_applications = length(applications)

q <- dbSendQuery(connect, statement = "SELECT DISTINCT distribution FROM experiment ORDER BY distribution")
distributions <- fetch(q)$distribution
print(distributions)
n_distributions = length(distributions)

q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment ORDER BY algorithm")
algorithms <- fetch(q)$algorithm
print(algorithms)

n_algorithms = length(algorithms)

n_budgets = 10
n_deadlines = 10


statement = sprintf("SELECT scoreBitString FROM experiment ORDER BY application, distribution, budget, deadline, runID, algorithmName")
print(statement)
q <- dbSendQuery(connect, statement = statement)
scoreBitString = fetch(q,-1)
# create 6-dimensional matrix
scoreBitStrings = array(scoreBitString$scoreBitString,  dim=c(n_algorithms, n_runIds, n_deadlines, n_budgets, n_distributions, n_applications))


maxScores = apply(scoreBitStrings, 2:6, max)
topScores = array(0,  dim=c(n_algorithms, n_runIds, n_deadlines, n_budgets, n_distributions, n_applications))
for (i in 1:n_algorithms) {
	topScores[i,,,,,] = scoreBitStrings[i,,,,,]>=maxScores
}
sumScores = apply(topScores,c(1,5,6),sum)

# number of top priority workflows
ntpw = function(x,maxBitString) {
	
	vec_x = as.numeric(unlist(strsplit(x,"")))
	vec_max = as.numeric(unlist(strsplit(maxBitString,"")))
	sum1s = 0
	for (i in 1:length(vec_x)) {
		if(vec_x[i]>=vec_max[i]) sum1s = sum1s+vec_x[i]
		else break
	}
	# sum of all 1s
	#sum1s = sum(as.numeric(unlist(strsplit(x,""))))
	sum1s
}
topScoresNTPW = array(0,  dim=c(n_algorithms, n_runIds, n_deadlines, n_budgets, n_distributions, n_applications))
for (i in 1:n_algorithms) {
	topScoresNTPW[i,,,,,] = mapply(ntpw,scoreBitStrings[i,,,,,],maxScores)
}
maxScoresNTPW = apply(topScoresNTPW, 2:6, max)
maxScoresNTPW = apply(maxScoresNTPW, 1:5, function(x) max(x,1))
for (i in 1:n_algorithms) {
	topScoresNTPW[i,,,,,] = topScoresNTPW[i,,,,,]/maxScoresNTPW
}
sumScoresNTPW = apply(topScoresNTPW,c(1,5,6),sum)



longest_common_prefix = function(stringarray) {
	len = length(stringarray)
	prefix = stringarray[1]
	for(i in 1:len) {
		s = stringarray[i]
		for(j in 1:nchar(s)) {
			if (substr(s,j,j)!=substr(prefix,j,j)) {
				prefix = substr(prefix,0,j-1)
				break
			}
		}
	}
	return(prefix)
}
prefixes = apply(scoreBitStrings, 2:6, longest_common_prefix)
# exponential score after cutting the common prefix
cut_score = function(x,prefix) {
	if(nchar(prefix)>=nchar(x)) return(1)
	x = substr(x,nchar(prefix)+1,nchar(x))
	vec_x = as.numeric(unlist(strsplit(x,"")))
	score = 0
	for (i in 1:length(vec_x)) {
		if(vec_x[i]>0) score = score + 2^(-i)
	}
	return(score)
}
cutScores = array(0,  dim=c(n_algorithms, n_runIds, n_deadlines, n_budgets, n_distributions, n_applications))
for (i in 1:n_algorithms) {
	cutScores[i,,,,,] = mapply(cut_score,scoreBitStrings[i,,,,,],prefixes)
}
maxCutScores = apply(cutScores, 2:6, max)
#maxCutScores = apply(maxCutScores, 1:5, function(x) max(x,1))
for (i in 1:n_algorithms) {
	cutScores[i,,,,,] = cutScores[i,,,,,]/maxCutScores
}
sumCutScores = apply(cutScores,c(1,5,6),sum)





pdf(file=paste(db_filename, "-distributions.pdf", sep=""), height=4, width=6, bg="white")
par(mfrow=c(2,3))

for (i in 1:n_applications) {
	print(applications[i])
	bar_data = sumScores[,,i]
	
	colnames(bar_data) = c("C", "PS", "PU", "US", "UU")
	rownames(bar_data) = algorithms
	# reorder algorithms and change to %
	bar_data = bar_data[c(1,3,2),]/10.0
	print(bar_data)	
	barplot(as.table(bar_data), beside = TRUE, xlab="Distribution", ylab = "Best Scores (%)", main = applications[i], col=gray( c(0.1,0.4,0.8) ),
	ylim = c(0,100), cex.names=0.8)
}
plot.new() 
plot.window(c(0,1), c(0,1))
legend("center", algorithms[c(1,3,2)], fill=gray( c(0.1,0.4,0.8) ))
dev.off()



pdf(file=paste(db_filename, "-distributions-N.pdf", sep=""), height=4, width=6, bg="white")
par(mfrow=c(2,3))

for (i in 1:n_applications) {
	print(applications[i])
	bar_data = sumScoresNTPW[,,i]
	
	colnames(bar_data) = c("C", "PS", "PU", "US", "UU")
	rownames(bar_data) = algorithms
	# reorder algorithms and change to %
	bar_data = bar_data[c(1,3,2),]/1000.0
	print(bar_data)	
	barplot(as.table(bar_data), beside = TRUE, xlab="Distribution", ylab = "Mean Scores N", main = applications[i], col=gray( c(0.1,0.4,0.8) ),
			ylim = c(0,1), cex.names=0.8)
}
plot.new() 
plot.window(c(0,1), c(0,1))
legend("center", algorithms[c(1,3,2)], fill=gray( c(0.1,0.4,0.8) ))

dev.off()



pdf(file=paste(db_filename, "-distributions-cut.pdf", sep=""), height=4, width=6, bg="white")
par(mfrow=c(2,3))

for (i in 1:n_applications) {
	print(applications[i])
	bar_data = sumCutScores[,,i]
	
	colnames(bar_data) = c("C", "PS", "PU", "US", "UU")
	rownames(bar_data) = algorithms
	# reorder algorithms and change to %
	bar_data = bar_data[c(1,3,2),]/1000.0
	print(bar_data)	
	barplot(as.table(bar_data), beside = TRUE, xlab="Distribution", ylab = "Mean Cut Score", main = applications[i], col=gray( c(0.1,0.4,0.8) ),
			ylim = c(0,1), cex.names=0.8)
}
plot.new() 
plot.window(c(0,1), c(0,1))
legend("center", algorithms[c(1,3,2)], fill=gray( c(0.1,0.4,0.8) ))

dev.off()