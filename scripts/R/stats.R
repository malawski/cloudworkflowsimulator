#path <- "/home/malawski/cloudworkflowsimulator/output/"

path <- ""



dpdsStats <- function(dag, budgets, deadline, max_scaling, runId=0) {
	files <- paste(path, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputSimple.txt", sep="")
	filesA <- paste(path, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputAware.txt", sep="")
	m <- matrix(0, length(files), length(read.table(files[1])$V2))
	mA <- matrix(0, length(files), length(read.table(files[1])$V2))
	deadlines <- read.table(files[1])$V1
	hours <- deadlines /3600
	for (i in 1:length(files)) {
		#print(files[i])
		m[i,] = read.table(files[i])$V2	
		mA[i,] = read.table(filesA[i])$V2	
	}
	#print(m)
	#print(mA)
	
	diff <- mA-m
	positive <- diff>0
	diffsum  <- rowSums(diff)
	diffmean <- apply(diff,1,mean)
	numpositive <- apply(diff,1, function (x) sum(x>0) )
	numnegative <- apply(diff,1, function (x) sum(x<0) )
	numzero <- apply(diff,1, function (x) sum(x==0) )
	total <- length(m[1,])
	percent_better <- numpositive/total * 100
	percent_worse <- numnegative/total * 100
	
	x <- cbind(budgets, numpositive, numnegative, numzero, total, percent_better, percent_worse)
	print(x)
	
	x_range = hours[length(hours)]
	y_range = max(m)
	
	png(filename=paste(dag, "h", deadline, "m", max_scaling, "run", runId, ".png", sep=""), height=800, width=800, bg="white")
	
	plot(hours, mA[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(0,y_range))	
	for (i in 1:length(files)) {
		lines(hours, mA[i,], type="l", col="blue", ann=FALSE)
		lines(hours, m[i,], type="l", pch=22, lty=5, col="red")
		legend(x_range*1.05, m[i,length(hours)], paste("$",budgets[i]), cex=0.8);
	
		# Create a title with a red, bold/italic font
	}
	title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)
	
	legend(x_range/2, 4, c("A-DPDS","DPDS"), cex=0.8, col=c("blue","red"), pch=21:22, lty=1:2);
	title(ylab="# dags finished")
	axis(2)
	axis(1)
	grid()
	box()
	dev.off()
}



#dpdsStats("Montage_1000.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 0)
#dpdsStats("Montage_1000.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 2)

#dpdsStats("Inspiral_1000.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 0)
#dpdsStats("Inspiral_1000.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 2)

#dpdsStats("Sipht_1000.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 0)
#dpdsStats("Sipht_1000.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 2)

#dpdsStats("Epigenomics_997.dag", c(40000.0, 32000.0, 24000.0, 16000.0, 8000.0), '10-1500', 0)
#dpdsStats("Epigenomics_997.dag", c(40000.0, 32000.0, 24000.0, 16000.0, 8000.0), '10-1500', 2)

#dpdsStats("CyberShake_1000.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 0)
#dpdsStats("CyberShake_1000.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 2)

#dpdsStats('psload_large.dag', c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-30', 0)
#dpdsStats('psload_large.dag', c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-30', 2)

#dpdsStats('psload_medium.dag', c(100.0, 80.0, 60.0, 40.0, 20.0), '1-50', 0)
#dpdsStats('psload_medium.dag', c(100.0, 80.0, 60.0, 40.0, 20.0), '1-50', 2)

#dpdsStats('psmerge_small.dag', c(10000.0, 8000.0, 6000.0, 4000.0, 2000.0), '5-150', 0)

#for (i in 0:9) {
#	dpdsStats('psmerge_small.dag', c(10000.0, 8000.0, 6000.0, 4000.0, 2000.0), '5-150', 2, i)	
#}


dpdsStats("MONTAGE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 0)
dpdsStats("MONTAGE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 2)

dpdsStats("CYBERSHAKE.n.1000.0.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 0)
dpdsStats("CYBERSHAKE.n.1000.0.dag", c(400.0, 320.0, 240.0, 160.0, 80.0), '1-20', 2)

dpdsStats("LIGO.n.1000.0.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 0)
dpdsStats("LIGO.n.1000.0.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 2)

dpdsStats("GENOME.n.1000.0.dag", c(40000.0, 32000.0, 24000.0, 16000.0, 8000.0), '10-1500', 0)
dpdsStats("GENOME.n.1000.0.dag", c(40000.0, 32000.0, 24000.0, 16000.0, 8000.0), '10-1500', 2)

dpdsStats("SIPHT.n.1000.0.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 0)
dpdsStats("SIPHT.n.1000.0.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 2)

