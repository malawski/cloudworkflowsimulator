csv_filename = "run-stretching-test-output.dat"

alldata = read.csv(csv_filename, colClasses=c("scorebits"="character"))

n_observations = nrow(alldata)

mydata = alldata[c("application", "scale", "distribution", "seed", "budget", "deadline", "algorithm", "scorebits")]

attach(mydata)
sorteddata = mydata[order(application, scale, distribution, seed, budget, deadline, algorithm),]
detach(mydata)

sorteddata$topscore = FALSE

attach(sorteddata)

i=1
while (i < n_observations) {
	
	s1 = scorebits[i]
	s2 = scorebits[i+1]
	s3 = scorebits[i+2]
	
	topscore[i] = (s1>=s2 & s1>=s3)
	topscore[i+1] = (s2>=s1 & s2>=s3)
	topscore[i+2] = (s3>=s2 & s3>=s1)
	
    # select a subtable of 3 rows
	#newdata <- sorteddata[i:(i+2),]
	#newdata$maxscore = max(newdata$scorebits)
	#newdata$topscore = newdata$scorebits>=newdata$maxscore
	
	#print (newdata$topscore)
	# fill the last column of original table
	#sorteddata$topscore[i:(i+2)] = newdata$topscore
	
	i = i+3

	print(i)
}

detach(sorteddata)
sorteddata$topscore = topscore
sorteddata$percscore = topscore/10.0

sumscore = aggregate( percscore ~ application:distribution:scale:algorithm, data=sorteddata, FUN=sum)
print(sumscore)
algorithms = levels(sumscore$algorithm)
nalgorithms = length(algorithms)
scales = unique(sumscore$scale)

distributions = levels(sumscore$distribution)
ndistributions = length(distributions)


pdf(file=paste(csv_filename, "-scaling-CYBERSHAKE.pdf", sep=""), height=4, width=6, bg="white")
par(mfrow=c(2,3))
for (j in 1:ndistributions) {
	
	plotdata = subset(sumscore, application=="CYBERSHAKE" & distribution==distributions[j])

	xrange = range(plotdata$scale)
	xrange[1]=0
	yrange = range(plotdata$percscore)
	yrange[1]=0

	plot(xrange, yrange, type="n", xlab="Stretching Factor",
		ylab="Best Scores (%)", main = distributions[j], xaxt = "n" ) 
    axis(1, scales)

	for (i in 1:nalgorithms) {
		linedata <- subset(plotdata, algorithm==algorithms[i])
		lines(linedata$scale, linedata$percscore, type="b", lwd=1.5, lty=2, col=i+1, pch=i)
	}
}
plot.new()
legend("center", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
dev.off()


pdf(file=paste(csv_filename, "-scaling-MONTAGE.pdf", sep=""), height=4, width=6, bg="white")
par(mfrow=c(2,3))
for (j in 1:ndistributions) {
	
	plotdata = subset(sumscore, application=="MONTAGE" & distribution==distributions[j])
	
	xrange = range(plotdata$scale)
	xrange[1]=0
	yrange = range(plotdata$percscore)
	yrange[1]=0
	
	plot(xrange, yrange, type="n", xlab="Stretching Factor",
			ylab="Best Scores (%)", main = distributions[j], xaxt = "n" ) 
	axis(1, scales)
	
	for (i in 1:nalgorithms) {
		linedata <- subset(plotdata, algorithm==algorithms[i])
		lines(linedata$scale, linedata$percscore, type="b", lwd=1.5, lty=2, col=i+1, pch=i)
	}
}
plot.new()
legend("center", c(algorithms[1:length(algorithms)]), cex=1, col=2:(length(algorithms)+1), pch=1:length(algorithms) )
dev.off()
