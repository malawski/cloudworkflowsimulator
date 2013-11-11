require(plotrix) # Required package

# see simple example: http://www.inside-r.org/packages/cran/plotrix/docs/gantt.chart

# if both vgidpos and vgridlab are specified,
# starts and ends don't have to be dates
info2<-list(labels=c("VM 1","VM 2","VM 1","VM 3","VM 3","VM 4","VM 2","VM 5","VM 4"),
		starts=c(8.1,8.7,13.0,9.1,11.6,9.0,13.6,9.3,14.2),
		ends=c(12.5,12.7,16.5,10.3,15.6,11.7,18.1,18.2,19.0))

gantt.chart(info2,vgridlab=8:19,vgridpos=8:19,
		main="A color for each interval - with borders",
		taskcolors=c(2,3,7,4,8,5,3,6,"purple"),border.col="black")


csv_filename = "/home/malawski/git/cloudworkflowsimulator-storage/task-info3.csv"
alldata = read.csv(csv_filename, sep = " ")

alldata <- alldata[order(alldata$vm),] 


# if needed to start from 1, but probably better not rename them to keep consistent with logs
#minvm = min(alldata$vm)
#alldata$vm <- alldata$vm - minvm + 1

info2<-list(labels=alldata$vm,		
		starts=alldata$startTaskTime, 
		ends=alldata$finishTaskTime)

maxFinish = max(alldata$finishTaskTime)

#png(file=paste(csv_filename, ".png", sep=""), width=1600, height=900, bg="white")
pdf(file=paste(csv_filename, ".pdf", sep=""), width=16, height=9, bg="white")

gantt.chart(info2,vgridlab=seq(0,maxFinish/3600, 1),vgridpos=seq(0,maxFinish, 3600),
		taskcolors =alldata$dag, main="Tasks on VMs",border.col="black")

dev.off()