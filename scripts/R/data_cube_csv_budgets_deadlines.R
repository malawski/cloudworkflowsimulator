
csv_filename = "run-finish-variations-test-0-output.dat"


alldata = read.csv(csv_filename, colClasses=c("scorebits"="character"))
alldata = alldata[alldata$runtimeVariance==0,]

attach(alldata)
failureRates = unique(failureRate)
#alldata$deadline = alldata$deadline/3600

runIds = unique(seed)
n_runIds=length(runIds)


print(n_runIds)


applications = levels(unique(application))

naapplications = length(applications)

distributions = levels(unique(distribution))
print(distributions)
ndistributions = length(distributions)

algorithms = levels(unique(algorithm))
print(algorithms)

nalgorithms = length(algorithms)

deadlines = unique(deadline)

detach(alldata)

mydata = alldata[c("application", "distribution", "seed", "budget", "deadline", "algorithm", "completed", "minDeadline", "maxDeadline")]

# normalize deadlines to the values 1, 2, 3, ..., 10 for averaging
mydata$deadline = round((mydata$deadline-mydata$minDeadline)/(mydata$maxDeadline-mydata$minDeadline)*90/10+1)

deadlines = unique(mydata$deadline)
print(deadline)


attach(mydata)
avgdata = aggregate( completed ~ application:distribution:deadline:algorithm, data=mydata, FUN=mean)
detach(mydata)

# renormalize deadlines to the values 0 to 1 for plotting
avgdata$deadline = (avgdata$deadline-1)/90*10



pdf(file=paste(csv_filename, "-deadlines-app-dist.pdf", sep=""), height=18, width=16, bg="white")
par(mfrow=c(5,5))
for (app in 1:naapplications) {
for (j in 1:ndistributions) {
	
	plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])
	
	xrange = range(plotdata$deadline)
	xrange[1]=0
	yrange = range(plotdata$completed)
	yrange[1]=0
	
	plot(xrange, yrange, type="n", xlab="Normalized deadline",
			ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
	legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
	
	for (i in 1:nalgorithms) {
		linedata <- subset(plotdata, algorithm==algorithms[i])
		lines(linedata$deadline, linedata$completed, type="b", lwd=1, lty=1, col=i+1, pch=i)
	}
}
}
dev.off()



mydata = alldata[c("application", "distribution", "seed", "budget", "deadline", "algorithm", "completed", "minBudget", "maxBudget", "maxDeadline")]

# normalize budgets to the values 1, 2, 3, ..., 10 for averaging
mydata$budget = round((mydata$budget-mydata$minBudget)/(mydata$maxBudget-mydata$minBudget)*90/10+1)

budgets = unique(mydata$budget)
print(budgets)


attach(mydata)
avgdata = aggregate( completed ~ application:distribution:budget:algorithm, data=mydata, FUN=mean)
detach(mydata)

# renormalize budgets to the values 0 to 1 for plotting
avgdata$budget = (avgdata$budget-1)/90*10



pdf(file=paste(csv_filename, "-budgets-app-dist.pdf", sep=""), height=18, width=16, bg="white")
par(mfrow=c(5,5))
for (app in 1:naapplications) {
	for (j in 1:ndistributions) {
		
		plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])
		
		xrange = range(plotdata$budget)
		xrange[1]=0
		yrange = range(plotdata$completed)
		yrange[1]=0
		
		plot(xrange, yrange, type="n", xlab="Normalized budget",
				ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
		legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
		
		for (i in 1:nalgorithms) {
			linedata <- subset(plotdata, algorithm==algorithms[i])
			lines(linedata$budget, linedata$completed, type="b", lwd=1, lty=1, col=i+1, pch=i)
		}
	}
}
dev.off()


