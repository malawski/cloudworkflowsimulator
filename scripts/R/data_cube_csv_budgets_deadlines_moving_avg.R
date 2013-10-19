
csv_filename = "run-finish-variations-test-0-output.dat"


alldata = read.csv(csv_filename, colClasses=c("scorebits"="character"))
alldata = alldata[alldata$runtimeVariance==0,]

attach(alldata)
failureRates = unique(failureRate)
alldata$deadline = alldata$deadline/3600

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

mydata = alldata[c("application", "distribution", "seed", "budget", "deadline", "algorithm", "completed")]

attach(mydata)
avgdata = aggregate( completed ~ application:distribution:deadline:algorithm, data=mydata, FUN=mean)
detach(mydata)



# plot only first application and distribution
app=1
j=1

pdf(file=paste(csv_filename, "-deadlines-app-dist-1-1.pdf", sep=""), height=5, width=5, bg="white")
plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])

plotdata = plotdata[order(plotdata$deadline),]
print(plotdata)

xrange = range(plotdata$deadline)
xrange[1]=0
yrange = range(plotdata$completed)
yrange[1]=0

plot(xrange, yrange, type="n", xlab="Deadline in hours",
		ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )

for (i in 1:nalgorithms) {
	linedata <- subset(plotdata, algorithm==algorithms[i])
	print(linedata)
	window=10
	filtered = filter(linedata$completed,rep(1/window,window), sides=2)
	linedata$ma = filtered
	lines(linedata$deadline, linedata$ma, type="b", lwd=1, lty=0, col=i+1, pch=i)
}
dev.off()


# plot all applications and distributions

pdf(file=paste(csv_filename, "-deadlines-app-dist-ma.pdf", sep=""), height=18, width=16, bg="white")
par(mfrow=c(5,5))
for (app in 1:naapplications) {
for (j in 1:ndistributions) {
	
	plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])
	plotdata = plotdata[order(plotdata$deadline),]
	
	
	xrange = range(plotdata$deadline)
	xrange[1]=0
	yrange = range(plotdata$completed)
	yrange[1]=0
	
	plot(xrange, yrange, type="n", xlab="Deadline in hours",
			ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
	legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
	
	for (i in 1:nalgorithms) {
		linedata <- subset(plotdata, algorithm==algorithms[i])
		print(linedata)
		window=20
		filtered = filter(linedata$completed,rep(1/window,window), sides=1)
		linedata$ma = filtered
		lines(linedata$deadline, linedata$ma, type="b", lwd=1, lty=0, col=i+1, pch=i)	}
}
}
dev.off()



mydata = alldata[c("application", "distribution", "seed", "budget", "deadline", "algorithm", "completed", "minBudget", "maxBudget", "maxDeadline")]
mydata$budget = (mydata$budget-mydata$minBudget)/(mydata$maxBudget-mydata$minBudget)

avgdata = aggregate(completed ~ application:distribution:budget:algorithm, data=mydata, FUN=mean)
detach(mydata)


# plot only first application and distribution
app=1
j=1

pdf(file=paste(csv_filename, "-budgets-app-dist-1-1.pdf", sep=""), height=5, width=5, bg="white")
plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])

plotdata = plotdata[order(plotdata$completed),]
print(plotdata)

xrange = range(plotdata$budget)
xrange[1]=0
yrange = range(plotdata$completed)
yrange[1]=0

plot(xrange, yrange, type="n", xlab="Budget in $",
		ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )

for (i in 1:nalgorithms) {
	linedata <- subset(plotdata, algorithm==algorithms[i])
	print(linedata)
	window=10
	filtered = filter(linedata$budget,rep(1/window,window), sides=2)
	linedata$ma = filtered
	lines(linedata$ma, linedata$completed, type="b", lwd=1, lty=0, col=i+1, pch=i)
}
dev.off()

# plot all applications and distributions

pdf(file=paste(csv_filename, "-budgets-app-dist-ma.pdf", sep=""), height=18, width=16, bg="white")
par(mfrow=c(5,5))
for (app in 1:naapplications) {
	for (j in 1:ndistributions) {
		
		plotdata = subset(avgdata, application==applications[app] & distribution==distributions[j])
		plotdata = plotdata[order(plotdata$budget),]
		
		xrange = range(plotdata$budget)
		xrange[1]=0
		yrange = range(plotdata$completed)
		yrange[1]=0
		
		plot(xrange, yrange, type="n", xlab="Budget in $",
				ylab="Completed Workflows", main = paste(c(applications[app],distributions[j])))
		legend("bottomright", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
		
		for (i in 1:nalgorithms) {
			linedata <- subset(plotdata, algorithm==algorithms[i])
			window=20
			filtered = filter(linedata$completed,rep(1/window,window), sides=1)
			linedata$ma = filtered
			lines(linedata$budget, linedata$ma, type="b", lwd=1, lty=0, col=i+1, pch=i)
		}
	}
}
dev.off()


