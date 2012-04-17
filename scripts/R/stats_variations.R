library(DBI)
library(RSQLite)
library(cwhmisc)



############################################################################################################################
# Start
############################################################################################################################



db_filename = "variations-50.sqlite"
driver<-dbDriver("SQLite")
connect<-dbConnect(driver, dbname = db_filename)
dbListTables(connect)


statement = sprintf("SELECT application, distribution, algorithmName, runtimeVariation AS variation, actualFinishTime/deadline AS ratio FROM experiment ORDER BY application, distribution, algorithmName")
print(statement)
q <- dbSendQuery(connect, statement = statement)
ratio = fetch(q,-1)

applications = unique(ratio$application)
print(applications)
n_applications = length(applications)

pdf(file=paste(db_filename, "-variations-application.pdf", sep=""), height=6, width=9, bg="white")
par(mfrow=c(2,3))
for (i in 1:n_applications) {
	print(applications[i])
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$application==applications[i],],main = applications[i],cex.names=0.8, ylim = c(0,2))
	abline(h = 1, col = "red")
}
plot.new() 
plot.window(c(0,1), c(0,1))
dev.off()



distributions = unique(ratio$distribution)
print(distributions)

pdf(file=paste(db_filename, "-variations-distribution.pdf", sep=""), height=6, width=9, bg="white")
par(mfrow=c(2,3))
for (distribution in distributions) {
	print(distribution)
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$distribution==distribution,],main = distribution,cex.names=0.8,ylim = c(0,2),ylab = "makespan / deadline")
	abline(h = 1, col = "red")
}
plot.new() 
plot.window(c(0,1), c(0,1))
dev.off()


variations = unique(ratio$variation)
print(variations)

pdf(file=paste(db_filename, "-variations-variation.pdf", sep=""), height=6, width=12, bg="white")
par(mfrow=c(1,7))
for (variation in variations) {
	print(variation)
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$variation==variation,],main = sprintf("± %2.0f %%",variation*100),cex.names=0.8,ylim = c(0,2),ylab = "makespan / deadline")
	abline(h = 1, col = "red")
}
dev.off()

pdf(file=paste(db_filename, "-variations-all.pdf", sep=""), height=16, width=10, bg="white")
par(mfrow=c(6,7))
for(application in applications) {
  for (variation in variations) {
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$variation==variation & ratio$application==application,],main = sprintf("%s, ± %2.0f %%",application, variation*100),cex.names=0.8,ylim = c(0,2),ylab = "makespan / deadline")
	abline(h = 1, col = "red")
  }
}
for (variation in variations) {
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$variation==variation,],main = sprintf("ALL ± %2.0f %%",variation*100),cex.names=0.8,ylim = c(0,2),ylab = "makespan / deadline")
	abline(h = 1, col = "red")
}
dev.off()


###### Plot cost/budget

statement = sprintf("SELECT application, distribution, algorithmName, runtimeVariation AS variation, cost/budget AS ratio FROM experiment ORDER BY application, distribution, algorithmName")
print(statement)
q <- dbSendQuery(connect, statement = statement)
ratio = fetch(q,-1)
pdf(file=paste(db_filename, "-variations-cost.pdf", sep=""), height=16, width=10, bg="white")
par(mfrow=c(6,7))
for(application in applications) {
	for (variation in variations) {
		boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$variation==variation & ratio$application==application & ratio$distribution=="uniform_unsorted",],cex.names=0.8,ylim = c(0,2),ylab = "cost / budget")
		abline(h = 1, col = "red")
	}
}
for (variation in variations) {
	boxplot(ratio~algorithmName, las = 2, data = ratio[ratio$variation==variation & ratio$distribution=="uniform_unsorted",],main = sprintf("ALL ± %2.0f %%",variation*100),cex.names=0.8,ylim = c(0,2),ylab = "cost / budget")
	abline(h = 1, col = "red")
}
dev.off()

#### scratchpad


pdf(file=paste(db_filename, "-variations-var-app.pdf", sep=""), height=22, width=20, bg="white")
par(mfrow=c(5,5))
for (distribution in distributions) {
  for (application in applications) {
	boxplot(ratio~algorithmName:variation, border = rainbow(3), col = "lightgrey", las = 2, data = ratio[ratio$application==application & ratio$distribution==distribution,],main = sprintf("%s, %s",application, distribution),cex.names=0.8, ylim = c(0,2),ylab = "cost / budget")
	abline(h = 1, col = "red")
  }
}
plot.new() 
plot.window(c(0,1), c(0,1))
dev.off()

