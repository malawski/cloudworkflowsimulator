library(DBI)
library(RSQLite)
library(gmp)

############################################################################################################################
# Global variables
############################################################################################################################


global_titles <<- ""
global_rankings_m <<- cbind(algorithms)
global_rankings_s <<- cbind(algorithms)
global_rankings_c <<- cbind(algorithms)
global_rankings_d <<- cbind(algorithms)

############################################################################################################################
# Functions
############################################################################################################################

algorithms_top <- function(list_scores) {
	
	list_max = do.call(pmax, list_scores)
	list_top = lapply(list_scores, function(x) x>=list_max)
	algorithms_top = lapply(list_top, sum)	
}


plot_series <- function(prefix, plot_title, application, budgets, algorithms, list_avg, hours, legend_position) {
	pdf(file=paste(prefix, application, ".pdf", sep=""), height=5, width=20, bg="white")
	par(mfrow=c(1,5))	
	#pdf(file=paste(prefix, dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=3, width=10, bg="white")
	#par(mfrow=c(2,1))
	x_max = hours[length(hours)]
	# create vectors for max and min values
	list_max = 1:n_algorithms
	list_min = 1:n_algorithms 
	
	for (i in 1:length(budgets)) {
		
		for(alg in 1:length(algorithms)) {
			list_max[alg]=max(list_avg[[alg]][i,])
			list_min[alg]=min(list_avg[[alg]][i,])
		}
		y_max = max(list_max)
		y_min = min(list_min)
		# print(y_max)
		# print(y_min)
		
		plot(hours, list_avg[[1]][1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_max*1.2), ylim=c(y_min,y_max))
		for(alg in 1:length(algorithms)){	
			lines(hours, list_avg[[alg]][i,], type="o", pch=alg, lty=5, col=alg)
		}
		
		title(main=paste("$",budgets[i]), col.main="black", font.main=4)
		legend(legend_position, c(algorithms[1:length(algorithms)]), cex=1, col=1:length(algorithms), pch=1:length(algorithms) )
		
		
		#legend(x_range*1.0, avg_m[i,length(hours)]+5, paste("$",budgets[i]), cex=1.2, bty="n");
		
		title(ylab=plot_title, cex.lab=1.2)
		title(xlab="deadline in hours", cex.lab=1.2)
		axis(2,cex.axis=1.2)
		axis(1,cex.axis=1.2)
		grid(col = "gray", lty = "dashed")
		box()
		
	}
	
	#legend("topright", c(algorithms$algorithm[1:6]), cex=1, col=1:6, pch=1:6 )
	mtext(application, side=3, outer=TRUE, line=-1.5) 
	dev.off()
}

# Plot selected column from the series
plot_selected <- function(prefix, plot_title, application, budget_id, algorithms, list_avg, hours, legend_position) {
	pdf(file=paste(prefix, application, ".pdf", sep=""), height=4, width=5, bg="white")
#	par(mfrow=c(1,5))	
	par(mar=c(4,4,0.5,0.1))
	#pdf(file=paste(prefix, dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=3, width=10, bg="white")
	#par(mfrow=c(2,1))
	x_max = hours[length(hours)]
	# create vectors for max and min values
	list_max = 1:n_algorithms
	list_min = 1:n_algorithms
	
	i = budget_id	
	
	for(alg in 1:length(algorithms)) {
		list_max[alg]=max(list_avg[[alg]][i,])
		list_min[alg]=min(list_avg[[alg]][i,])
	}
	y_max = max(list_max)
	y_min = min(list_min)
	# print(y_max)
	# print(y_min)
	
	plot(hours, list_avg[[1]][1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_max*1.2), ylim=c(y_min,y_max))
	for(alg in 1:length(algorithms)){	
		lines(hours, list_avg[[alg]][i,], type="o", pch=alg, lty=5, col=alg)
	}
	
	#title(main=paste("budget = $",budgets[i]), col.main="black", font.main=4)
	legend(legend_position, c("SPSS","WADPDS","DPDS"), cex=1, col=c(2,1,3), pch=c(2,1,3) )
	
	
	#legend(x_range*1.0, avg_m[i,length(hours)]+5, paste("$",budgets[i]), cex=1.2, bty="n");
	
	title(ylab=plot_title, cex.lab=1.2)
	title(xlab="deadline in hours", cex.lab=1.2)
	axis(2,cex.axis=1.2)
	axis(1,cex.axis=1.2)
	grid(col = "gray", lty = "dashed")
	box()
	
	
	
	#legend("topright", c(algorithms$algorithm[1:6]), cex=1, col=1:6, pch=1:6 )
#	mtext(application, side=3, outer=TRUE, line=-1.5) 
	dev.off()
}





############################################################################################################################
# Start
############################################################################################################################




driver<-dbDriver("SQLite")
#connect<-dbConnect(driver, dbname = "fractions-nodelays-50-2.sqlite")
#connect<-dbConnect(driver, dbname = "fractions-nodelays.sqlite")
connect<-dbConnect(driver, dbname = "pareto-nodelays-all.sqlite")
#connect<-dbConnect(driver, dbname = "test3.sqlite")
dbListTables(connect)

#q <- dbSendQuery(connect, statement = "PRAGMA synchronous = OFF")
#fetch(q)
#q <- dbSendQuery(connect, statement = "PRAGMA journal_mode = MEMORY")
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

q <- dbSendQuery(connect, statement = "SELECT DISTINCT application FROM experiment ")
applications <- fetch(q)$application
print(applications)

n_applications = length(applications)


############################################################################################################################
# Main loop
############################################################################################################################


#for(app in 1:1) {
for(app in 1:n_applications) {
	application = applications[app]
	
	print(application)
	
	q <- dbSendQuery(connect, statement = sprintf("SELECT DISTINCT budget FROM experiment WHERE application = '%s' ORDER BY budget", application))
	budgets <- fetch(q)$budget
	
	print(budgets)
	
	n_budgets = length(budgets)
	
	print(n_budgets)
	
	
	q <- dbSendQuery(connect, statement = sprintf("SELECT DISTINCT deadline FROM experiment WHERE application = '%s' ORDER BY deadline", application))
	deadlines <- fetch(q)$deadline
	hours <- deadlines/3600
	print(deadlines)
	
	n_deadlines = length(deadlines)
	
	print(n_deadlines)
	
	
	q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment WHERE algorithm LIKE '%P%S' ")
	algorithms <- fetch(q)$algorithm
	print(algorithms)
	
	n_algorithms = length(algorithms)
	
	# create list of matrices for number of dags
	list_m <- vector("list", n_algorithms) 
	
	# create list of matrices for sum runtimes (sizes)
	list_s <- vector("list", n_algorithms) 
	
	# create list of matrices for costs
	list_c <- vector("list", n_algorithms) 
	
	# create list of matrices for exponential scores using bigq
	list_e <- vector("list", n_algorithms) 
	# create list of matrices for exponential scores using double
	list_d <- vector("list", n_algorithms) 
	
	
	# read numbers of dags completed
	for(alg in 1:n_algorithms){
		statement = sprintf("SELECT finished FROM experiment WHERE application = '%s' AND algorithmName = '%s' ORDER BY deadline, budget, runId", 
				application,
				algorithms[[alg]])
		print(statement)
		q <- dbSendQuery(connect, statement = statement)
		finished = fetch(q,-1)
		list_m[[alg]] = array(finished$finished,  dim=c(n_runIds, n_budgets, n_deadlines))
	}
	
	# read sum of runtimes and compute costs
	for(alg in 1:n_algorithms){
		statement = sprintf("SELECT SUM(size)/3600.0 as size FROM sizes JOIN experiment ON experiment.id = sizes.experiment_id WHERE application = '%s' AND algorithmName = '%s' GROUP by deadline, budget, runId", 
				application,
				algorithms[[alg]])
		print(statement)
		q <- dbSendQuery(connect, statement = statement)
		sizes = fetch(q,-1)
		list_s[[alg]] = array(sizes$size,  dim=c(n_runIds, n_budgets, n_deadlines))
		# initialize costs with 0s
		list_c[[alg]] = array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
		# compute effective cost as budget/size
		for (i in 1:n_budgets) {
			list_c[[alg]][,i,]=budgets[i]/list_s[[alg]][,i,]
		}
	}
	
	# read priorities and compute score
#	for(alg in 1:n_algorithms){
#		statement = sprintf("SELECT group_concat(priority) as priorities FROM priorities JOIN experiment ON experiment.id = priorities.experiment_id WHERE application = '%s' AND algorithmName = '%s' GROUP by deadline, budget, runId", 
#				application,
#				algorithms[[alg]])
#		print(statement)
#		q <- dbSendQuery(connect, statement = statement)
#		priorities = fetch(q,-1)$priorities
#		scores = matrix.bigq(nrow=1, ncol=length(priorities))
#		for(i in 1:length(priorities)) {
#			scores[1,i] <- sum(as.bigq(2^as.numeric(unlist(strsplit(priorities[i], ",")))))
#		}
#		scores = scores/2^(max_priority)
#		list_e[[alg]] = array(scores,  dim=c(n_runIds, n_budgets, n_deadlines))
#		list_d[[alg]] = array(as.double(scores),  dim=c(n_runIds, n_budgets, n_deadlines))
#	}
	
	
	# create lists of averaged matrices
	list_avg_m <- vector("list", n_algorithms) # list of number of workflows finished
	list_avg_s <- vector("list", n_algorithms) # list of sum of runtimes (sizes)
	list_avg_c <- vector("list", n_algorithms) # list of costs
#	list_avg_e <- vector("list", n_algorithms) # list of exponential scores
	for(i in 1:n_algorithms){
		list_avg_m[[i]] <- apply(list_m[[i]],c(2,3),mean)
		list_avg_s[[i]] <- apply(list_s[[i]],c(2,3),mean)
		list_avg_c[[i]] <- apply(list_c[[i]],c(2,3),mean)
#		list_avg_e[[i]] <- apply(list_d[[i]],c(2,3),mean) # use list in double for average values
	}
	
	# plot averaged results
	plot_series("plot", "# dags finished", application, budgets, algorithms, list_avg_m, hours, "bottomright" )
	
	# plot averaged sum runtimes
	plot_series("size", "total runtime in hours", application, budgets, algorithms, list_avg_s, hours, "bottomright" )
	
	# plot averaged costs
	plot_series("cost", "computing cost in $/h", application, budgets, algorithms, list_avg_c, hours, "topright" )
	
	# plot averaged exponential scores
#	plot_series("score", "exponential score", application, budgets, algorithms, list_avg_e, hours, "bottomright" )
	
	# compute table with rankings
	#list_max = pmax(list_d[[1]],list_d[[2]],list_d[[3]],list_d[[4]],list_d[[5]],list_d[[6]])
	

	# plot selected images, etc. 2nd budget for LIGO
	plot_selected("size-selected-", "total runtime in hours", application, 2, algorithms, list_avg_s, hours, "bottomright" )
	plot_selected("cost-selected", "computing cost in $/h", application, 2, algorithms, list_avg_c, hours, "topright" )
	


	global_rankings_m <<-cbind(global_rankings_m, algorithms_top(list_m))
	global_rankings_s <<-cbind(global_rankings_s, algorithms_top(list_s))
	global_rankings_c <<-cbind(global_rankings_c, algorithms_top(list_c))	
	global_rankings_d <<-cbind(global_rankings_d, algorithms_top(list_d))
	global_titles <<- paste(global_titles, application, " ")	
	
}


#print(global_titles)
#print(global_rankings_m)
#print(global_rankings_s)
# does not make sense, should reverse the condition to min
#print(global_rankings_c)
#print(global_rankings_d)


#"DPDS"      400 267 227 134 65
#"WADPDS"    561 302 292 322 65 
#"SPSS"      288 522 226 350 213



