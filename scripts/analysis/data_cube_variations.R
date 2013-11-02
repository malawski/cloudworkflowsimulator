

############################################################################################################################
# Start
############################################################################################################################

#csv_filename = "run-failures-test-0-output.dat"
#csv_filename = "run-scaling-failures-test-0-output.dat"
#csv_filename = "run-scaling-1-failures-test-0-output.dat"
#csv_filename = "run-finish-failures-test-0-output.dat"
csv_filename = "run-finish-variations-test-0-output.dat"


data = read.csv(csv_filename, colClasses=c("scorebits"="character"))
#data = data[data$failureRate==0,]
#data = data[data$runtimeVariance==0,]

runtimeVariances = unique(data$runtimeVariance)

n_runtimeVariances = length(runtimeVariances)

runIds = unique(data$seed)
n_runIds=length(runIds)


print(n_runIds)


applications = levels(unique(data$application))

n_applications = length(applications)

distributions = levels(unique(data$distribution))
print(distributions)
n_distributions = length(distributions)

algorithms = levels(unique(data$algorithm))
print(algorithms)

n_algorithms = length(algorithms)

n_budgets = 10
n_deadlines = 10



# create 7-dimensional matrix
cube_dimensions = c(n_algorithms, n_runIds, n_deadlines, n_budgets, n_distributions, n_applications, n_runtimeVariances)
scoreBitString = data[order(data$runtimeVariance, data$application, data$distribution, data$budget, data$deadline, data$seed, data$algorithm),]$scorebits
scoreBitStrings = array(scoreBitString,  dim=cube_dimensions)


maxScores = apply(scoreBitStrings, 2:7, max)
topScores = array(0,  dim=cube_dimensions)
for (i in 1:n_algorithms) {
	topScores[i,,,,,,] = scoreBitStrings[i,,,,,,]>=maxScores
}
# sum scores, grouping by algorithm, application and variance
sumScores = apply(topScores,c(1,6,7),sum)



pdf(file=paste(csv_filename, "-distributions-variances.pdf", sep=""), height=2, width=7, bg="white")
par(mfrow=c(1,3))
par(mar=c(4,4,2,0))
#select only cybershake and ligo
for (i in c(1,3)) {
	print(applications[i])
	bar_data = sumScores[,i,]
	
	colnames(bar_data) = runtimeVariances*100
	rownames(bar_data) = algorithms
	# reorder algorithms and change to %
	bar_data = bar_data[c(1,3,2),]/50.0
	print(bar_data)	
	barplot(as.table(bar_data), beside = TRUE, xlab="Estimate error (%)",  main = applications[i], 
			col=gray( c(0.1,0.4,0.8) ),
	ylim = c(0,100), cex.names=1.0)
	if (i==1) {title(ylab="Best Scores (%)")}
}
plot.new()
plot.window(c(0,1), c(0,1))
legend("left", algorithms[c(1,3,2)], fill=gray( c(0.1,0.4,0.8) ))
dev.off()


