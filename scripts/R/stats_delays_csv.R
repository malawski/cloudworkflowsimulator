



############################################################################################################################
# Start
############################################################################################################################



csv_filename = "run-finish-delays-test-0-output"

data = read.csv(paste(csv_filename,".dat", sep = ""))

# reorder algorithms
data$algorithm = factor(data$algorithm,c("DPDS","WADPDS","SPSS"))

delays = unique(data$delay)
print(delays)

applications = levels(unique(data$application))
print(applications)
distributions = levels(unique(data$distribution))
print(distributions)
#algorithms = levels(unique(data$algorithm))
algorithms = c("DPDS","WADPDS","SPSS")
print(algorithms)

pdf(file=paste(csv_filename, "-jobfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (delay in delays) {
	print(delays)
	boxplot(jobfinish/deadline~algorithm, las = 2, data = data[data$delay==delay,],main = sprintf("%2.0f",delay),cex.names=0.8,ylim = c(0,2),ylab = "jobfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-dagfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (delay in delays) {
	print(delays)
	boxplot(dagfinish/deadline~algorithm, las = 2, data = data[data$delay==delay,],main = sprintf("%2.0f",delay),cex.names=0.8,ylim = c(0,2),ylab = "dagfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-vmfinish.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (delay in delays) {
	print(delays)
	boxplot(vmfinish/deadline~algorithm, las = 2, data = data[data$delay==delay,],main = sprintf("%2.0f",delay),cex.names=0.8,ylim = c(0,2),ylab = "vmfinish / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-vmfinish-deadline.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (delay in delays) {
	print(delays)
	boxplot((vmfinish-deadline)/3600.0~algorithm, las = 2, data = data[data$delay==delay,],main = sprintf("%2.0f",delay),cex.names=0.8,ylim = c(-4,4),ylab = "vmfinish - deadline in hours")
	abline(h = 1, col = "red")
}
dev.off()


pdf(file=paste(csv_filename, "-cost.pdf", sep=""), height=4, width=12, bg="white")
par(mfrow=c(1,7))
for (delay in delays) {
	print(delays)
	boxplot(cost/budget~algorithm, las = 2, data = data[data$delay==delay,],main = sprintf("%2.0f",delay),cex.names=0.8,ylim = c(0,2),ylab = "cost / budget")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(csv_filename, "-cost-spss.pdf", sep=""), height=4, width=4, bg="white")
boxplot(cost/budget~delay, las = 2, data = data[data$algorithm=="SPSS",],
		main = sprintf("SPSS"),cex.names=0.8,ylim = c(0,2),
		ylab = "cost / budget",xlab = "delay", 
		names=sprintf("%1.0f", delays))
abline(h = 1, col = "red")
dev.off()


pdf(file=paste(csv_filename, "-cost-var-app.pdf", sep=""), height=22, width=20, bg="white")
par(mfrow=c(5,5))
for (distribution in distributions) {
	for (application in applications) {
		boxplot(cost/budget~algorithm:delay, border = rainbow(3), col = "lightgrey", 
				las = 2, data = data[data$application==application & data$distribution==distribution,],
				main = sprintf("%s, %s",application, distribution),cex.names=0.8, 
				ylim = c(0,2),ylab = "cost / budget")
		abline(h = 1, col = "red")
	}
}
dev.off()

pdf(file=paste(csv_filename, "-dagfinish-var-app.pdf", sep=""), height=22, width=20, bg="white")
par(mfrow=c(5,5))
for (distribution in distributions) {
	for (application in applications) {
		boxplot(dagfinish/deadline~algorithm:delay, border = rainbow(3), col = "lightgrey", 
				las = 2, data = data[data$application==application & data$distribution==distribution,],
				main = sprintf("%s, %s",application, distribution),cex.names=0.8, 
				ylim = c(0,2),ylab = "dagfinish / deadline")
		abline(h = 1, col = "red")
	}
}
dev.off()






pdf(file=paste(csv_filename, "-cost-montage-uniform_unsorted.pdf", sep=""), height=5, width=5, bg="white")
distribution = "uniform_unsorted"
application  = "MONTAGE"
par(mar=c(4.5,4,0.01,0.01), oma=c(1,0,0,0),xpd=T)
boxplot(cost/budget~algorithm:delay, col=gray( c(0.3,0.6,0.9) ), 
		las = 2, 
		data = data[data$application==application & data$distribution==distribution,],
		xaxt='n',
		#outline = FALSE,
		range = 0, # whiskers exend to the max
#		main = "Runtime estimate error", 
		ylim = c(0,2),ylab = "Cost / Budget",
#		xlab = "Runtime estimate error",
		cex.axis = 0.8
#		names=sprintf("%s", outer(algorithms, sprintf("%2.0f %%",runtimeVariances*100), FUN='paste')))
#		names=sprintf("%s", unlist(lapply(runtimeVariances*100, function(x) c(" ", paste(sprintf("%2.0f %%",x),"{")," ")))))
)
axis(1, 0:9*3+0.5, rep('', 10),tck=1, col=grey(0.1)) 
axis(1, 1:24, rep(algorithms, 8),tck=0,las=2,cex.axis = 0.7, pos=0.0, lty=0) 
axis(1, 0:9*3+0.5, rep('', 10),tck=-0.15) 
axis(1, 0:7*3+2.0, tck=0, sprintf("%2.0f",delays), cex.axis = 0.9, lty=0, pos=-0.27) 
legend(0, 2.05, algorithms, fill = gray( c(0.3,0.6,0.9)), cex=0.75)
#par(xpd=T)
text(11,-0.54,"Provisioning delay in seconds")
par(xpd=F)
abline(h = 1, col = "red")
dev.off()


pdf(file=paste(csv_filename, "-dagfinish-montage-uniform_unsorted.pdf", sep=""), height=5, width=5, bg="white")
distribution = "uniform_unsorted"
application  = "MONTAGE"
par(mar=c(4.5,4,0.01,0.01), oma=c(1,0,0,0),xpd=T)
boxplot(dagfinish/deadline~algorithm:delay, col=gray( c(0.3,0.6,0.9) ), 
		las = 2, 
		data = data[data$application==application & data$distribution==distribution,],
		xaxt='n',
#		outline = FALSE,
		range = 0, # whiskers exend to the max
#		main = "Runtime estimate error", 
		ylim = c(0,2),ylab = "Makespan / Deadline",
#		xlab = "Runtime estimate error",
		cex.axis = 0.8
#		names=sprintf("%s", unlist(lapply(runtimeVariances*100, function(x) c(" ", paste(sprintf("%2.0f %%",x),"{")," ")))))
)
axis(1, 0:9*3+0.5, rep('', 10),tck=1, col=grey(0.1)) 
axis(1, 1:24, rep(algorithms, 8),tck=0,las=2,cex.axis = 0.7, pos=0.0, lty=0) 
axis(1, 0:9*3+0.5, rep('', 10),tck=-0.15) 
axis(1, 0:7*3+2.0, tck=0, sprintf("%2.0f",delays), cex.axis = 0.9, lty=0, pos=-0.27) 
legend(0, 2.05, algorithms, fill = gray( c(0.3,0.6,0.9)), cex=0.75)
#par(xpd=T)
text(11,-0.54,"Provisioning delay in seconds")
par(xpd=F)
abline(h = 1, col = "red")
dev.off()



pdf(file=paste(csv_filename, "-cost-montage.pdf", sep=""), height=5, width=5, bg="white")
application  = "MONTAGE"
par(mar=c(4.5,4,0.01,0.01), oma=c(1,0,0,0),xpd=T)
boxplot(cost/budget~algorithm:delay, col=gray( c(0.3,0.6,0.9) ), 
		las = 2, 
		data = data[data$application==application,],
		xaxt='n',
		#outline = FALSE,
		range = 0, # whiskers exend to the max
#		main = "Runtime estimate error", 
		ylim = c(0,2),ylab = "Cost / Budget",
#		xlab = "Runtime estimate error",
		cex.axis = 0.8
#		names=sprintf("%s", outer(algorithms, sprintf("%2.0f %%",runtimeVariances*100), FUN='paste')))
#		names=sprintf("%s", unlist(lapply(runtimeVariances*100, function(x) c(" ", paste(sprintf("%2.0f %%",x),"{")," ")))))
)
axis(1, 0:9*3+0.5, rep('', 10),tck=1, col=grey(0.1)) 
axis(1, 1:24, rep(algorithms, 8),tck=0,las=2,cex.axis = 0.7, pos=0.0, lty=0) 
axis(1, 0:9*3+0.5, rep('', 10),tck=-0.15) 
axis(1, 0:7*3+2.0, tck=0, sprintf("%2.0f",delays), cex.axis = 0.9, lty=0, pos=-0.27) 
legend(0, 2.05, algorithms, fill = gray( c(0.3,0.6,0.9)), cex=0.75)
#par(xpd=T)
text(11,-0.54,"Provisioning delay in seconds")
par(xpd=F)
abline(h = 1, col = "red")
dev.off()


pdf(file=paste(csv_filename, "-dagfinish-montage.pdf", sep=""), height=5, width=5, bg="white")
application  = "MONTAGE"
par(mar=c(4.5,4,0.01,0.01), oma=c(1,0,0,0),xpd=T)
boxplot(dagfinish/deadline~algorithm:delay, col=gray( c(0.3,0.6,0.9) ), 
		las = 2, 
		data = data[data$application==application,],
		xaxt='n',
#		outline = FALSE,
		range = 0, # whiskers exend to the max
#		main = "Runtime estimate error", 
		ylim = c(0,2),ylab = "Makespan / Deadline",
#		xlab = "Runtime estimate error",
		cex.axis = 0.8
#		names=sprintf("%s", unlist(lapply(runtimeVariances*100, function(x) c(" ", paste(sprintf("%2.0f %%",x),"{")," ")))))
)
axis(1, 0:9*3+0.5, rep('', 10),tck=1, col=grey(0.1)) 
axis(1, 1:24, rep(algorithms, 8),tck=0,las=2,cex.axis = 0.7, pos=0.0, lty=0) 
axis(1, 0:9*3+0.5, rep('', 10),tck=-0.15) 
axis(1, 0:7*3+2.0, tck=0, sprintf("%2.0f",delays), cex.axis = 0.9, lty=0, pos=-0.27) 
legend(0, 2.05, algorithms, fill = gray( c(0.3,0.6,0.9)), cex=0.75)
#par(xpd=T)
text(11,-0.54,"Provisioning delay in seconds")
par(xpd=F)
abline(h = 1, col = "red")
dev.off()