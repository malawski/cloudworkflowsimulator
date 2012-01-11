#path <- "/home/malawski/cloudworkflowsimulator/output/"

path <- ""



dpdsStats <- function(dag, budgets, deadline, max_scaling, runIds={0}) {
	
	n_runIds = length(runIds)
	n_budgets = length(budgets)
	first_file <- paste(path, dag, "b", budgets[1], ".0h", deadline, "m", max_scaling, ".0", "run", runIds[1], "-outputSimple.txt", sep="")
	n_deadlines = length(read.table(first_file)$V2)
	print(n_runIds)
	print(n_budgets)
	print(n_deadlines)
	
	m <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
	mA <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
	mS <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
	
	for (runId in runIds) {
	
		files <- paste(path, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputSimple.txt", sep="")
		filesA <- paste(path, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputAware.txt", sep="")
		filesS <- paste(path, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputSPSS.txt", sep="")

		deadlines <- read.table(files[1])$V1
		hours <- deadlines /3600
		for (i in 1:n_budgets) {
			print(files[i])
			m[runId+1,i,] = read.table(files[i])$V2	
			mA[runId+1,i,] = read.table(filesA[i])$V2
			mS[runId+1,i,] = read.table(filesS[i])$V2
		}
	}
		print(m)
	
	avg_m <- apply(m,c(2,3),mean)
	avg_mA <- apply(mA,c(2,3),mean)
	avg_mS <- apply(mS,c(2,3),mean)
	
	dag_cost <- 1/(avg_m/budgets)
	dag_costA <- 1/(avg_mA/budgets)
	
	print(avg_m[,1:5])
	print(dag_cost[,1:5])
	
	print(avg_m)
	
	diff <- avg_mA-avg_m
	positive <- diff>0
	diffsum  <- rowSums(diff)
	diffmean <- apply(diff,1,mean)
	numpositive <- apply(diff,1, function (x) sum(x>0) )
	numnegative <- apply(diff,1, function (x) sum(x<0) )
	numzero <- apply(diff,1, function (x) sum(x==0) )
	total <- n_deadlines
	percent_better <- numpositive/total * 100
	percent_worse <- numnegative/total * 100
	
	x <- cbind(budgets, numpositive, numnegative, numzero, total, percent_better, percent_worse)
	print(x)
	
	x_range = hours[length(hours)]
	y_range = max(max(avg_m),max(avg_mA))
	
	print(y_range)
	
	# plot averaged results
	pdf(file=paste(dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=8, width=8, bg="white")
	
	plot(hours, avg_mA[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(0,y_range))	
	for (i in 1:n_budgets) {
		lines(hours, avg_m[i,], type="o", pch=22, lty=5, col="red")
		lines(hours, avg_mA[i,], type="o", col="blue", ann=FALSE)
		lines(hours, avg_mS[i,], type="o", col="green", ann=FALSE)
		
		legend(x_range*1.0, avg_m[i,length(hours)]+5, paste("$",budgets[i]), cex=1.5, bty="n");
	}
	title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)
	
	legend(x_range/2, 5.5, c("WA-DPDS","DPDS", "SPSS"), cex=1, col=c("blue","red", "green"), pch=22, );
	title(ylab="# dags finished", cex.lab=1.5)
	title(xlab="deadline in hours", cex.lab=1.5)
	axis(2,cex.axis=1.5)
	axis(1,cex.axis=1.5)
	grid(col = "gray", lty = "dashed")
	box()
	dev.off()
	
	# plot differences
	y_max = max(diff)
	y_min = min(diff)
	png(filename=paste("diff-", dag, "h", deadline, "m", max_scaling, ".png", sep=""), height=400, width=800, bg="white")
	
	plot(hours, diff[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(y_min,y_max)*1.1)	
	for (i in 1:n_budgets) {
		lines(hours, diff[i,], type="p", col=i, ann=FALSE)
	}
	title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)
	
	legend(x_range*1.1, y_max/2, paste("$",budgets),  pch=1, cex=1.2, col=1:n_budgets);
	title(ylab="difference in # dags finished")
	title(xlab="deadline in hours")
	axis(2)
	axis(1)
	grid()
	box()
	dev.off()
	
	# plot costs
	y_max = max(dag_cost)
	y_min = min(dag_cost)
	png(filename=paste("cost-", dag, "h", deadline, "m", max_scaling, ".png", sep=""), height=800, width=800, bg="white")
	
	plot(hours, dag_cost[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(y_min,y_max))	
	for (i in 1:n_budgets) {
		lines(hours, dag_cost[i,], type="p", col=i, pch=1, ann=FALSE)
		lines(hours, dag_costA[i,], type="p", col=i, pch=2, ann=FALSE)
	}
	title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)
	
	legend(x_range*1.1, (y_min+y_max)/2, paste("$",budgets), lty=1, cex=0.8, col=1:n_budgets);
	legend(x_range*1.1, (y_min+y_max)/2.2, c("A-DPDS","DPDS"), pch=c(2,1), cex=0.8);
	
	title(ylab="cost per DAG")
	title(xlab="deadline in hours")
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


#dpdsStats("MONTAGE.n.1000.8.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 0)
#dpdsStats("MONTAGE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 2, 0:9)

dpdsStats("CYBERSHAKE.n.1000.8.dag", c(40.0, 60.0, 80.0, 100.0, 120.0), '1-20', 0)
#dpdsStats("CYBERSHAKE.n.1000.8.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 0)
#dpdsStats("CYBERSHAKE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 2, 0:9)

#dpdsStats("LIGO.n.1000.1.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 0)
#dpdsStats("LIGO.n.1000.0.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 2, 0:9)

#dpdsStats("GENOME.n.1000.1.dag", c(4000.0, 8000.0, 12000.0, 16000.0, 20000.0), '10-1500', 0)
#dpdsStats("GENOME.n.1000.0.dag", c(4000.0, 8000.0, 12000.0, 16000.0, 20000.0), '10-1500', 2, 0:9)

#dpdsStats("SIPHT.n.1000.1.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 0)
#dpdsStats("SIPHT.n.1000.0.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 2, 0:9)


