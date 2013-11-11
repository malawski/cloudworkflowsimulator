ensemble_data <- read.table("../scripts/R/ensemble-sizes.txt", header=F)
print(ensemble_data)
pdf(file="ensemble-pareto.pdf", height=4, width=4, bg="white")
hist(ensemble_data$V1, col="lightblue", freq = TRUE, breaks = c(0, 51, 101, 201, 301, 401, 501, 601, 901, 1001), 
		xlab="workflow size", ylab="frequency", main = "distribution of workflow sizes")
dev.off()