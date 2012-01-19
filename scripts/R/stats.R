library(gmp)

global_titles <<- ""
global_rankings <<- cbind(c(0,0,0))
global_rankings_scores <<- cbind(c(0,0,0))
global_rankings_escores <<- cbind(c(0,0,0))

global_totals <<- cbind(c(0,0,0))
global_stotals <<- cbind(c(0,0,0))

score_weight <- function(p) {
        return (1/(p+1))
}

readExpScores <- function(prefix, dag, budgets, deadline, n_deadlines, max_scaling, suffix) {
	
	n_budgets = length(budgets)
	
	
	s <- matrix.bigq(nrow=n_budgets, ncol=n_deadlines)
	
	
	runId=0
	score_files <- paste(path, prefix, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, suffix, sep="")
	
	for (i in 1:n_budgets) {
		lines = readLines(score_files[i], n=-1)
		#print(lines)
		deadline_id = 0
		for (line in lines) {
			deadline_id = deadline_id +1
			scores = as.numeric(unlist(strsplit(line, " ")))
			n_scores = length(scores)
			score = as.bigq(0)
			#print(score)
			# we skip the first value since it contains a deadline
			for (p in scores[2:n_scores]) {
				score = score + 1/as.bigq(2^p)
				#score = score + 1/as.bigq(p+1)
				
			}                                                                                                                                           
			#print(score)                                                                                                                               
			s[i,deadline_id] = score                                                                                                            
			
		}
	}
	
	return(s)
}

readScores <- function(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, suffix) {
	n_budgets = length(budgets)
	n_runIds = length(runIds)
	
		s <- array(0,  dim=c(length(runIds), n_budgets, n_deadlines))
	
	for (runId in runIds) {
		score_files <- paste(path, prefix, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, suffix, sep="")
		
		for (i in 1:n_budgets) {
			lines = readLines(score_files[i], n=-1)
			print(lines)
			deadline_id = 0
			for (line in lines) {
				deadline_id = deadline_id +1
				scores = as.numeric(unlist(strsplit(line, " ")))
				n_scores = length(scores)
				score = 0
				#print(score)
				# we skip the first value since it contains a deadline
				for (p in scores[2:n_scores]) {
					#score = score + 1/(p+1)                                                                                                            
					#score = score + 2^(-p)                                                                                                              
					score = score + p/3600.0
					
				}                                                                                                                                           
				print(score)                                                                                                                               
				s[runId+1,i,deadline_id] = score                                                                                                            
				#s[runId+1,i,deadline_id] = budgets[i]/score                                                                                                
				
			}
		}
	}
	return(s)
}



dpdsStats <- function(title, prefix, dag, budgets, deadline, max_scaling, runIds={0}) {

        n_runIds = length(runIds)
        n_budgets = length(budgets)
        first_file <- paste(path, prefix, dag, "b", budgets[1], ".0h", deadline, "m", max_scaling, ".0", "run", runIds[1], "-outputSimple.txt", sep="")
        n_deadlines = length(read.table(first_file)$V2)
        print(n_runIds)
        print(n_budgets)
        print(n_deadlines)


        # matrix with number of dags finished per (runId, budget, deadline)
        # m - Simple, mA - Aware, mS - SPSS
        m <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
        mA <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))
        mS <- array(0,  dim=c(n_runIds, n_budgets, n_deadlines))


        # read files with numbers of dags completed

        for (runId in runIds) {

                files <- paste(path, prefix, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputSimple.txt", sep="")
                filesA <- paste(path, prefix, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputAware.txt", sep="")
                filesS <- paste(path, prefix, dag, "b", budgets, ".0h", deadline, "m", max_scaling, ".0", "run", runId, "-outputSPSS.txt", sep="")

                deadlines <- read.table(files[1])$V1
                hours <- deadlines /3600
                for (i in 1:n_budgets) {
                        print(files[i])
                        m[runId+1,i,] = read.table(files[i])$V2
                        mA[runId+1,i,] = read.table(filesA[i])$V2
                        mS[runId+1,i,] = read.table(filesS[i])$V2
                }
        }

        # matrix with score per (runId, budget, deadline)
        # s - Simple, sA - Aware, sS - SPSS

        #s = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-prioritiesSimple.txt")
        #sA = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-prioritiesAware.txt")
        #sS = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-prioritiesSPSS.txt")

		se = readExpScores(prefix, dag, budgets,  deadline, n_deadlines, max_scaling, "-prioritiesSimple.txt")
		seA = readExpScores(prefix, dag, budgets, deadline, n_deadlines, max_scaling, "-prioritiesAware.txt")
		seS = readExpScores(prefix, dag, budgets, deadline, n_deadlines, max_scaling, "-prioritiesSPSS.txt")
		
        s = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-sizesSimple.txt")
        sA = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-sizesAware.txt")
        sS = readScores(prefix, dag, budgets, runIds, deadline, n_deadlines, max_scaling, "-sizesSPSS.txt")


        #print(s)
        #print(sA)
        #print(sS)



        avg_m <- apply(m,c(2,3),mean)
        avg_mA <- apply(mA,c(2,3),mean)
        avg_mS <- apply(mS,c(2,3),mean)

        avg_s <- apply(s,c(2,3),mean)
        avg_sA <- apply(sA,c(2,3),mean)
        avg_sS <- apply(sS,c(2,3),mean)


        dag_cost <- 1/(avg_m/budgets)
        dag_costA <- 1/(avg_mA/budgets)

        #print(avg_m[,1:5])
        #print(dag_cost[,1:5])

        #print(avg_m)

        diff <- avg_mS-avg_mA
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
        print(paste(title, ": Differences in number of dags"))
        print(x)

        #diff <- avg_sS-avg_sA
        #positive <- diff>0
        #diffsum  <- rowSums(diff)
        #diffmean <- apply(diff,1,mean)
        #numpositive <- apply(diff,1, function (x) sum(x>0) )
        #numnegative <- apply(diff,1, function (x) sum(x<0) )
        #numzero <- apply(diff,1, function (x) sum(x==0) )
        #total <- n_deadlines
        #percent_better <- numpositive/total * 100
        #percent_worse <- numnegative/total * 100

        #x <- cbind(budgets, numpositive, numnegative, numzero, total, percent_better, percent_worse)
        #print(paste(title, ": Differences in score"))
        #print(x)

		# compute rankings for num dags
		topDPDS <- avg_m>=avg_mA & avg_m>=avg_mS
		count_topDPDS <-rowSums(topDPDS)
		totalDPDS <- rowSums(avg_m)
		
		topWADPDS <- avg_mA>=avg_m & avg_mA>=avg_mS
		count_topWADPDS <-rowSums(topWADPDS)
		totalWADPDS <- rowSums(avg_mA)
		
		topSPSS <- avg_mS>=avg_m & avg_mS>=avg_mA
		count_topSPSS <-rowSums(topSPSS)
		totalSPSS <- rowSums(avg_mS)
		
		x <- cbind(budgets, count_topDPDS, count_topWADPDS, count_topSPSS)
		print(x)
		print(colSums(x))
		global_rankings <<-cbind(global_rankings, colSums(x)[2:4])

		x <- cbind(budgets, totalDPDS, totalWADPDS, totalSPSS)
		print(x)
		print(colSums(x))
		global_totals <<-cbind(global_totals, colSums(x)[2:4])
		


		# compute rankings for scores
		topDPDS <- avg_s>=avg_sA & avg_s>=avg_sS
		count_topDPDS <-rowSums(topDPDS)
		stotalDPDS <- rowSums(avg_s)
		
		topWADPDS <- avg_sA>=avg_s & avg_sA>=avg_sS
		count_topWADPDS <-rowSums(topWADPDS)
		stotalWADPDS <- rowSums(avg_sA)
		
		topSPSS <- avg_sS>=avg_s & avg_sS>=avg_sA
		count_topSPSS <-rowSums(topSPSS)
		stotalSPSS <- rowSums(avg_sS)
		
		x <- cbind(budgets, count_topDPDS, count_topWADPDS, count_topSPSS)
		print(x)
		print(colSums(x))
		global_titles <<- paste(global_titles, title, " ")
		global_rankings_scores <<-cbind(global_rankings_scores, colSums(x)[2:4])
		
		
		x <- cbind(budgets, stotalDPDS, stotalWADPDS, stotalSPSS)
		print(x)
		print(colSums(x))
		global_stotals <<-cbind(global_stotals, colSums(x)[2:4])

		# compute rankings for exp scores
		etopDPDS <- se>=seA & se>=seS
		ecount_topDPDS <-rowSums(etopDPDS)
		
		etopWADPDS <- seA>=se & seA>=seS
		ecount_topWADPDS <-rowSums(etopWADPDS)
		
		etopSPSS <- seS>=se & seS>=seA
		ecount_topSPSS <-rowSums(etopSPSS)
		
		ex <- cbind(budgets, ecount_topDPDS, ecount_topWADPDS, ecount_topSPSS)
		print(ex)
		print(colSums(ex))
		global_rankings_escores <<-cbind(global_rankings_escores, colSums(ex)[2:4])
		
		
		
        x_range = hours[length(hours)]



        # plot averaged results
        pdf(file=paste(prefix, dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=7, width=3, bg="white")
        par(mfrow=c(2,1))

        #pdf(file=paste(prefix, dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=3, width=10, bg="white")
        #par(mfrow=c(1,5))


        #title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)

        for (i in 1:n_budgets) {
                y_range = max(max(avg_m[i,]),max(avg_mA[i,]),max(avg_mS[i,]))
                y_min = min(min(avg_m[i,]),min(avg_mA[i,]),min(avg_mS[i,]))
                print(y_range)


                plot(hours, avg_mA[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(y_min,y_range))
                lines(hours, avg_m[i,], type="o", pch=1, lty=5, col="red")
                lines(hours, avg_mA[i,], type="o", pch=2, col="blue", ann=FALSE)
                lines(hours, avg_mS[i,], type="o", pch=3, col="green", ann=FALSE)

                title(main=paste("$",budgets[i]), col.main="black", font.main=4)

                legend(x_range*1.0, avg_m[i,length(hours)]+5, paste("$",budgets[i]), cex=1.2, bty="n");


                title(ylab="# dags finished", cex.lab=1.2)
                title(xlab="deadline in hours", cex.lab=1.2)
                axis(2,cex.axis=1.2)
                axis(1,cex.axis=1.2)
                grid(col = "gray", lty = "dashed")
                box()

        }
        legend("bottomright", c("DPDS", "WA-DPDS", "SPSS"), cex=1, col=c("red", "blue","green"), pch=1:3 );
        mtext(title, side=3, outer=TRUE, line=-1.5) 
        dev.off()


        # plot scores
        #pdf(file=paste(prefix, "score-", dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=7, width=3, bg="white")
        #par(mfrow=c(2,1))

        pdf(file=paste(prefix, "score-", dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=3, width=10, bg="white")
        par(mfrow=c(1,5))

        #title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)

        for (i in 1:n_budgets) {
                y_range = max(max(avg_s[i,]),max(avg_sA[i,]),max(avg_sS[i,]))
                y_min = min(min(avg_s[i,]),min(avg_sA[i,]),min(avg_sS[i,]))
                print(y_range)


                plot(hours, avg_sA[1,]*1.2, type="n", col="blue", axes=FALSE, ann=FALSE, xlim=c(0,x_range*1.2),  ylim=c(y_min,y_range))
                lines(hours, avg_s[i,], type="o", pch=1, lty=5, col="red")
                lines(hours, avg_sA[i,], type="o", pch=2, col="blue", ann=FALSE)
                lines(hours, avg_sS[i,], type="o", pch=3, col="green", ann=FALSE)

                title(main=paste("$",budgets[i]), col.main="black", font.main=4)

                legend(x_range*1.0, avg_s[i,length(hours)]+5, paste("$",budgets[i]), cex=1.2, bty="n");

                title(ylab="exp score", cex.lab=1.2)
                #title(ylab="score", cex.lab=1.2)
                #title(ylab="total runtime in hours", cex.lab=1.2)
                #title(ylab="computing cost in $/h", cex.lab=1.2)

                title(xlab="deadline in hours", cex.lab=1.2)
                axis(2,cex.axis=1.2)
                axis(1,cex.axis=1.2)
                grid(col = "gray", lty = "dashed")
                box()

        }
        legend("bottomright", c("DPDS", "WA-DPDS", "SPSS"), cex=0.8, col=c("red", "blue","green"), pch=1:3 );
        #legend("topright", c("DPDS", "WA-DPDS", "SPSS"), cex=0.8, col=c("red", "blue","green"), pch=1:3 );
        mtext(title, side=3, outer=TRUE, line=-1.5) 
        dev.off()


        # plot averaged results vs budgets
        pdf(file=paste(prefix, "budget-", dag, "h", deadline, "m", max_scaling, ".pdf", sep=""), height=10, width=10, bg="white")

        #par(mfrow=c(5,5))
        #title(main=paste(dag, " max scaling ", max_scaling), col.main="black", font.main=4)

        for (i in 1:length(hours)) {
                pdf(file=paste(prefix, "budget-", dag, "h", deadline, "h", i, "m", max_scaling, ".pdf", sep=""), height=4, width=4, bg="white")
                x_range = budgets[length(budgets)]
                y_range = max(max(avg_m[,i]),max(avg_mA[,i]),max(avg_mS[,i]))
                print(y_range)

                barplot(rbind(avg_mA[,i],avg_mA[,i],avg_mS[,i]),beside=TRUE, col=c("red", "blue","green"), xlab="budget in $", ylab="# dags finished", cex.lab=0.8,
                                names.arg = budgets, cex.axis=0.8, cex.names=0.8)

                title(main=paste("deadline = ",hours[i], "h"), col.main="black", font.main=2)
                legend("topleft", c("DPDS", "WA-DPDS", "SPSS"), bty="n", fill=c("red", "blue","green"), cex = 0.8);
                mtext(title, side=3, outer=TRUE, line=-1.5) 
                dev.off()

        }





        # plot differences
        y_max = max(diff)
        y_min = min(diff)
        png(filename=paste(prefix, "diff-", dag, "h", deadline, "m", max_scaling, ".png", sep=""), height=400, width=800, bg="white")

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
        png(filename=paste(prefix, "cost-", dag, "h", deadline, "m", max_scaling, ".png", sep=""), height=800, width=800, bg="white")

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
#       dpdsStats('psmerge_small.dag', c(10000.0, 8000.0, 6000.0, 4000.0, 2000.0), '5-150', 2, i)
#}



#path <- "/home/malawski/cloudworkflowsimulator/output/"

path <- ""
prefix <- "pareto-"

#dpdsStats("Montage", prefix, "MONTAGE.n.1000.8.dag", c(20.0, 80.0), '1-20', 0)
dpdsStats("Montage", prefix, "MONTAGE.n.1000.8.dag", c(20.0, 30.0, 50.0, 60.0, 80.0), '1-20', 0)
#dpdsStats("MONTAGE.n.1000.8.dag", c(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0), '1-20', 0)
#dpdsStats("MONTAGE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 2, 0:9)

#dpdsStats("CyberShake", prefix, "CYBERSHAKE.n.1000.8.dag", c(50.0, 100.0), '1-20', 0)
dpdsStats("CyberShake", prefix, "CYBERSHAKE.n.1000.8.dag", c(30.0, 50.0, 80.0, 100.0, 140.0), '1-20', 0)
#dpdsStats("CYBERSHAKE.n.1000.8.dag", c(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 120.0, 140.0), '1-20', 0)
#dpdsStats("CYBERSHAKE.n.1000.8.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 0)
#dpdsStats("CYBERSHAKE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 2, 0:9)

#dpdsStats("LIGO", prefix, "LIGO.n.1000.8.dag", c(600.0, 1200.0), '1-40', 0)
dpdsStats("LIGO", prefix, "LIGO.n.1000.8.dag", c(400.0, 600.0, 800.0, 1000.0, 1200.0), '1-40', 0)
#dpdsStats("LIGO.n.1000.8.dag", c(200.0, 400.0, 600.0, 800.0, 1000.0, 1200.0, 1400.0, 1600.0, 1800.0, 2000.0), '1-40', 0)
#dpdsStats("LIGO.n.1000.0.dag", c(2000.0, 1600.0, 1200.0, 800.0, 400.0), '1-40', 2, 0:9)

#dpdsStats("Epigenomics", prefix, "GENOME.n.1000.8.dag", c(6000.0, 10000.0), '100-1500', 0)
dpdsStats("Epigenomics", prefix, "GENOME.n.1000.8.dag", c(4000.0, 6000.0, 8000.0, 10000.0, 12000.0), '100-1500', 0)
#dpdsStats("GENOME.n.1000.8.dag", c(2000.0, 4000.0, 6000.0, 8000.0, 10000.0, 12000.0, 14000.0, 16000.0, 18000.0, 20000.0), '100-1500', 0)
#dpdsStats("GENOME.n.1000.0.dag", c(4000.0, 8000.0, 12000.0, 16000.0, 20000.0), '10-1500', 2, 0:9)


#dpdsStats("SIPHT", prefix, "SIPHT.n.1000.8.dag",  c(400.0, 1000.0), '5-50', 0)
dpdsStats("SIPHT", prefix, "SIPHT.n.1000.8.dag",  c(200.0, 400.0, 600.0, 800.0, 1000.0), '5-50', 0)
#dpdsStats("SIPHT.n.1000.8.dag",  c(200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0), '5-50', 0)
#dpdsStats("SIPHT.n.1000.0.dag",  c(1000.0, 800.0, 600.0, 400.0, 200.0), '1-50', 2, 0:9)

#prefix <- "constant-"
#dpdsStats("Montage", prefix, "MONTAGE.n.1000.0.dag", c(40.0, 80.0, 120.0, 160.0, 200.0), '1-20', 0)
#dpdsStats("CyberShake", prefix, "CYBERSHAKE.n.1000.0.dag", c(50.0, 150.0, 250.0, 350.0, 450.0), '1-20', 0)
#pdsStats("LIGO", prefix, "LIGO.n.1000.0.dag", c(500.0, 1000.0, 1500.0, 2000.0, 2500.0), '1-40', 0)
#dpdsStats("Epigenomics", prefix, "GENOME.n.1000.0.dag", c(4000.0, 6000.0, 8000.0, 10000.0, 12000.0), '100-1500', 0)
#dpdsStats("SIPHT", prefix, "SIPHT.n.1000.0.dag",  c(500.0, 1000.0, 1500.0, 2000.0, 2500.0), '50-50', 0)




global_rankings<<-cbind(global_rankings, rowSums(global_rankings))
global_rankings_scores<<-cbind(global_rankings_scores, rowSums(global_rankings_scores))
global_rankings_escores<<-cbind(global_rankings_escores, rowSums(global_rankings_escores))
global_totals<<-cbind(global_totals, rowSums(global_totals))
global_stotals<<-cbind(global_stotals, rowSums(global_stotals))


print(global_titles)
print(global_rankings)
print(global_rankings_scores)
print(global_rankings_escores)
print(global_totals)
print(global_stotals)

