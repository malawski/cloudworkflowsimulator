library(DBI)
library(RSQLite)
library(gmp)



############################################################################################################################
# Functions
############################################################################################################################

algorithms_top <- function(list_scores) {
	
	#list_max = pmax(list_d[[1]],list_d[[2]],list_d[[3]],list_d[[4]],list_d[[5]],list_d[[6]])
	
	list_max = do.call(pmax, list_scores)
	list_top = lapply(list_scores, function(x) x>=list_max)
	algorithms_top = lapply(list_top, sum)	
}


plot_series <- function(prefix, plot_title, application, budgets, algorithms, list_avg, hours, legend_position) {
	print(application)
#	pdf(file=paste(prefix, application, ".pdf", sep=""), height=5, width=20, bg="white")
#	par(mfrow=c(1,5))	
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
		mtext(application, side=3, outer=FALSE, line=0) 	
		
		
	}
	
	#legend("topright", c(algorithms$algorithm[1:6]), cex=1, col=1:6, pch=1:6 )

#	dev.off()
}



############################################################################################################################
# Start
############################################################################################################################




driver<-dbDriver("SQLite")
connect<-dbConnect(driver, dbname = "pareto-dilated-nodelays.sqlite")
#connect<-dbConnect(driver, dbname = "pareto-dilated4.sqlite")
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

q <- dbSendQuery(connect, statement = "SELECT DISTINCT application FROM experiment ")
applications <- fetch(q)$application
print(applications)

n_applications = length(applications)

############################################################################################################################
# Global variables
############################################################################################################################

global_titles <<- vector("list", n_applications) 
global_rankings_m <<- vector("list", n_applications) 
global_rankings_s <<- vector("list", n_applications) 
global_rankings_c <<- vector("list", n_applications) 
global_rankings_d <<- vector("list", n_applications) 


for(app in 1:n_applications) {
	global_titles[[app]] = ""
	global_rankings_m[[app]] = cbind(algorithms)
	global_rankings_s[[app]] = cbind(algorithms)
	global_rankings_c[[app]] = cbind(algorithms)
	global_rankings_d[[app]] = cbind(algorithms)
}





############################################################################################################################
# Main loop
############################################################################################################################

#ilatations = c(1,2,4,8)
dilatations = 0:10
#dilatations = 0:3
dilatations = 2^dilatations


#for(app in 1:1) {
for(app in 1:n_applications) {
	
	application = applications[app]
	print(application)
	
	# if we want to merge all plots of application
#	pdf(file=paste("plot-all-", application, ".pdf", sep=""), height=5*length(dilatations), width=20, bg="white")
	pdf(file=paste("size-all-", application, ".pdf", sep=""), height=5*length(dilatations), width=20, bg="white")
	par(mfrow=c(length(dilatations),5))	
	# endif
	
	
	
	for (taskDilatation in dilatations) {
		
	
	q <- dbSendQuery(connect, statement = sprintf("SELECT DISTINCT budget FROM experiment WHERE application = '%s' AND taskDilatation = '%s' ORDER BY budget", application, taskDilatation))
	budgets <- fetch(q)$budget
	
	print(budgets)
	
	n_budgets = length(budgets)
	
	print(n_budgets)
	
	
	q <- dbSendQuery(connect, statement = sprintf("SELECT DISTINCT deadline FROM experiment WHERE application = '%s' AND taskDilatation = '%s' ORDER BY deadline", application, taskDilatation))
	deadlines <- fetch(q)$deadline
	hours <- deadlines/3600
	print(deadlines)
	
	n_deadlines = length(deadlines)
	
	print(n_deadlines)
	
	
	q <- dbSendQuery(connect, statement = "SELECT DISTINCT algorithmName as algorithm FROM experiment WHERE algorithm LIKE '%P%S' ORDER BY algorithm")
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
		statement = sprintf("SELECT finished FROM experiment WHERE application = '%s' AND taskDilatation = '%s' AND algorithmName = '%s' ORDER BY deadline, budget, runId", 
				application,
				taskDilatation, 
				algorithms[[alg]])
		print(statement)
		q <- dbSendQuery(connect, statement = statement)
		finished = fetch(q,-1)
		list_m[[alg]] = array(finished$finished,  dim=c(n_runIds, n_budgets, n_deadlines))
	}
	
	# read sum of runtimes and compute costs
	for(alg in 1:n_algorithms){
		statement = sprintf("SELECT SUM(size)/3600.0 as size FROM sizes JOIN experiment ON experiment.id = sizes.experiment_id WHERE application = '%s' AND taskDilatation = '%s' AND algorithmName = '%s' GROUP by deadline, budget, runId", 
				application,
				taskDilatation,
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
	for(alg in 1:n_algorithms){
		statement = sprintf("SELECT group_concat(priority) as priorities FROM priorities JOIN experiment ON experiment.id = priorities.experiment_id WHERE application = '%s' AND taskDilatation = '%s' AND algorithmName = '%s' GROUP by deadline, budget, runId", 
				application,
				taskDilatation,
				algorithms[[alg]])
		print(statement)
		q <- dbSendQuery(connect, statement = statement)
		priorities = fetch(q,-1)$priorities
		scores = matrix.bigq(nrow=1, ncol=length(priorities))
		for(i in 1:length(priorities)) {
			scores[1,i] <- sum(as.bigq(2^as.numeric(unlist(strsplit(priorities[i], ",")))))
		}
		scores = scores/2^(max_priority)
		#list_e[[alg]] = array(scores,  dim=c(n_runIds, n_budgets, n_deadlines))
		list_d[[alg]] = array(as.double(scores),  dim=c(n_runIds, n_budgets, n_deadlines))
	}
	
	
	# create lists of averaged matrices
	list_avg_m <- vector("list", n_algorithms) # list of number of workflows finished
	list_avg_s <- vector("list", n_algorithms) # list of sum of runtimes (sizes)
	list_avg_c <- vector("list", n_algorithms) # list of costs
	list_avg_e <- vector("list", n_algorithms) # list of exponential scores
	for(i in 1:n_algorithms){
		list_avg_m[[i]] <- apply(list_m[[i]],c(2,3),mean)
		list_avg_s[[i]] <- apply(list_s[[i]],c(2,3),mean)
		list_avg_c[[i]] <- apply(list_c[[i]],c(2,3),mean)
		list_avg_e[[i]] <- apply(list_d[[i]],c(2,3),mean) # use list in double for average values
	}
	
	# plot averaged results
	#plot_series("plot", "# dags finished", paste(application,"x", taskDilatation, sep=""), budgets, algorithms, list_avg_m, hours, "bottomright" )
	
	# plot averaged sum runtimes
	plot_series("size", "total runtime in hours", paste(application,"x", taskDilatation, sep=""), budgets, algorithms, list_avg_s, hours, "bottomright" )
	
	# plot averaged costs
	#plot_series("cost", "computing cost in $/h", paste(application,"x", taskDilatation, sep=""), budgets, algorithms, list_avg_c, hours, "topright" )
	
	# plot averaged exponential scores
	#plot_series("score", "exponential score", paste(application,"x", taskDilatation, sep=""), budgets, algorithms, list_avg_e, hours, "bottomright" )
	
	# compute table with rankings
	
	
	global_rankings_m[[app]] = cbind(global_rankings_m[[app]], algorithms_top(list_m))
	global_rankings_s[[app]] =cbind(global_rankings_s[[app]], algorithms_top(list_s))
	global_rankings_c[[app]] =cbind(global_rankings_c[[app]], algorithms_top(list_c))	
	global_rankings_d[[app]] =cbind(global_rankings_d[[app]], algorithms_top(list_d))
	global_titles[[app]] = paste(global_titles[[app]], paste(application,"x", taskDilatation, sep=""), " ")	
	
}

dev.off()

}




plot_rankings <- function(file_prefix, application_name, plot_title, gm) {
	
	pdf(file=paste(file_prefix, application_name, ".pdf", sep=""), height=5, width=5, bg="white")
	
	gm_sub = gm[,1:length(dilatations)+1]
	gt = matrix(as.double(gm_sub), ncol=length(dilatations))
	colnames(gt) = dilatations
	rownames(gt) = algorithms
	print(gt)
	
	#barplot(as.table(gt), beside = TRUE, xlab="stretching", col=rainbow(6))
	par(xpd=FALSE,oma=c(0,0,0,4)) 
	plot(dilatations, gt[1,], ylab = "# best scores", xlab="stretching", xaxt = "n", col=1, type="n", ylim=c(min(gt), max(gt)), log = "x", xlog=TRUE)
	axis(1, at=dilatations, cex.axis=0.8)
	for (alg in 1:n_algorithms) {
		lines(dilatations, gt[alg,], col=alg, type="o", pch=alg, lty=alg, xlog=TRUE, xaxt = "n")
	}
	title(paste(application_name," ", plot_title))
	abline(v=dilatations, col="gray", lty="dotted")	
	grid(nx=NA,ny=NULL,col="gray", lty="dotted")
	par(xpd=NA,oma=c(0,0,0,4)) 
	legend(max(dilatations)*1.5, max(gt), algorithms, col=1:alg, pch=1:alg, lty=1:alg ,cex=0.6)
	dev.off()
}

# plt global rankings based on all runs

for(app in 1:n_applications) {
	print(global_titles[[app]])
	print(global_rankings_m[[app]])
	print(global_rankings_s[[app]])
	# does not make sense, should reverse the condition to min
	#print(global_rankings_c)
	print(global_rankings_d[[app]])
	plot_rankings("rankings-dags-",applications[app], "# dags finished", global_rankings_m[[app]])	
	plot_rankings("rankings-sizes-",applications[app],"total runtime in hours", global_rankings_s[[app]])	
	plot_rankings("rankings-exp-",applications[app], "exponential score", global_rankings_d[[app]])		
}
