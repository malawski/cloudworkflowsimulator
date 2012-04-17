



############################################################################################################################
# Start
############################################################################################################################



#csv_filename = "run-failures-0-output.dat"
#csv_filename = "run-failures-test-0-output.dat"
#csv_filename = "run-scaling-failures-test-0-output.dat"
#csv_filename = "run-scaling-1-failures-test-0-output.dat"
#csv_filename = "run-finish-failures-test-0-output.dat"
csv_filename = "run-finish-variations-test-0-output.dat"

data = read.csv(csv_filename)

failureRates = unique(data$failureRate)
print(failureRates)

pdf(file=paste(csv_filename, "-jobfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (failureRate in failureRates) {
	print(failureRates)
	boxplot(jobfinish/deadline~algorithm, las = 2, data = data[data$failureRate==failureRate,],main = sprintf("%2.0f %%",failureRate*100),cex.names=0.8,ylim = c(0,2),ylab = "jobfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-dagfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (failureRate in failureRates) {
	print(failureRates)
	boxplot(dagfinish/deadline~algorithm, las = 2, data = data[data$failureRate==failureRate,],main = sprintf("%2.0f %%",failureRate*100),cex.names=0.8,ylim = c(0,2),ylab = "dagfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-vmfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (failureRate in failureRates) {
	print(failureRates)
	boxplot(vmfinish/deadline~algorithm, las = 2, data = data[data$failureRate==failureRate,],main = sprintf("%2.0f %%",failureRate*100),cex.names=0.8,ylim = c(0,2),ylab = "vmfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-vmfinish-deadline.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (failureRate in failureRates) {
	print(failureRates)
	boxplot((vmfinish-deadline)/3600.0~algorithm, las = 2, data = data[data$failureRate==failureRate,],main = sprintf("%2.0f %%",failureRate*100),cex.names=0.8,ylim = c(-4,4),ylab = "vmfinish - deadline in hours")
	abline(h = 1, col = "red")
}
dev.off()


pdf(file=paste(csv_filename, "-cost.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (failureRate in failureRates) {
	print(failureRates)
	boxplot(cost/budget~algorithm, las = 2, data = data[data$failureRate==failureRate,],main = sprintf("%2.0f %%",failureRate*100),cex.names=0.8,ylim = c(0,2),ylab = "cost / budget")
	abline(h = 1, col = "red")
}
dev.off()

