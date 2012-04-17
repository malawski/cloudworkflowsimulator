library(gmp)

a <- {0:100}
b <- {0:101}

#a <- c(0,1,3,5,8,9)
#b <- c(0,1,2,3,4,5,6,7,8,9)

max=101

print(a)
print(b)

w1a <- as.bigq(2^a)
w2a <- as.bigq(2^(max-a))
w1b <- as.bigq(2^b)
w2b <- as.bigq(2^(max-b))

#w1a <- as.double(2^a)
#w2a <- as.double(2^(max-a))
#w1b <- as.double(2^b)
#w2b <- as.double(2^(max-b))



w1a = 1/w1a
w1b = 1/w1b


print(w1a)
print(w2a)
print(w1b)
print(w2b)



scorew1a <- sum(w1a)
scorew1b <- sum(w1b)

scorew2a <- sum(w2a)/2^max
scorew2b <- sum(w2b)/2^max



print(scorew1a)
print(scorew2a)

print(scorew1b)
print(scorew2b)


print(as.double(scorew1a))
print(as.double(scorew2a))

print(as.double(scorew1b))
print(as.double(scorew2b))

diff1 = scorew1b-scorew1a
diff2 = (sum(w2b)-sum(w2a))/2^max

print(diff1)
print(diff2)

print(as.double(diff1))
print(as.double(diff2))



priorities = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,100"

bp <- as.bigq(2^as.numeric(unlist(strsplit(priorities, ","))))
print (bp)
dp = as.double(bp)
print(sum(bp))





scoreBitStrings = array(c(	"1010","1011","1000",
							"1110","1111","1100"), dim=c(3,2))
maxScores = apply(scoreBitStrings, 2, max)
# number of top priority workflows
ntpw = function(x,maxBitString) {
	# find the position of the first 0 starting from the position of the first 1 in max score
	first1position = cpos(maxBitString,"1")
	if (is.na(first1position)) {return(0)}
	result = cpos(x,"0",first1position)-first1position
	if (is.na(result)) {result = nchar(x)-cpos(maxBitString,"1")+1}
	result
}
topScoresNTPW = array(0,  dim=c(3,2))
for (i in 1:3) {
	topScoresNTPW[i,] = mapply(ntpw,scoreBitStrings[i,],maxScores)
}
print(scoreBitStrings)
print(maxScores)
print(topScoresNTPW)




scoreBitStrings = array(c(	"1010","1011","1000",
							"1110","1111","1100",
							"1101","1011","0011"
							), dim=c(3,3))
maxScores = apply(scoreBitStrings, 2, max)
# number of top priority workflows
ntpw = function(x,maxBitString) {
	
	vec_x = as.numeric(unlist(strsplit(x,"")))
	vec_max = as.numeric(unlist(strsplit(maxBitString,"")))
	sum1s = 0
	for (i in 1:length(vec_x)) {
		if(vec_x[i]>=vec_max[i]) sum1s = sum1s+vec_x[i]
		else break
	}
	# sum of all 1s
	#sum1s = sum(as.numeric(unlist(strsplit(x,""))))
	sum1s
}
print(scoreBitStrings)
topScoresNTPW = array(0,  dim=c(3,3))
for (i in 1:3) {
	topScoresNTPW[i,] = mapply(ntpw,scoreBitStrings[i,],maxScores)
}

print(maxScores)
print(topScoresNTPW)




longest_common_prefix = function(stringarray) {
	len = length(stringarray)
	prefix = stringarray[1]
	for(i in 1:len) {
		s = stringarray[i]
		for(j in 1:nchar(s)) {
			if (substr(s,j,j)!=substr(prefix,j,j)) {
				prefix = substr(prefix,0,j-1)
				break
			}
		}
	}
	return(prefix)
}
scoreBitStrings = array(c(	"1010","1011","1000",
							"1110","1111","1100",
							"1101","1101","1101"
		), dim=c(3,3))
prefixes = apply(scoreBitStrings, 2, longest_common_prefix)
# exponential score after cutting the common prefix
cut_score = function(x,prefix) {
	if(nchar(prefix)>=nchar(x)) return(1)
	x = substr(x,nchar(prefix)+1,nchar(x))
	vec_x = as.numeric(unlist(strsplit(x,"")))
	score = 0
	for (i in 1:length(vec_x)) {
		if(vec_x[i]>0) score = score + 2^(-i)
	}
	return(score)
}
print(scoreBitStrings)
topScoresNTPW = array(0,  dim=c(3,3))
for (i in 1:3) {
	topScoresNTPW[i,] = mapply(cut_score,scoreBitStrings[i,],prefixes)
}
print(prefixes)
print(topScoresNTPW)




print(longest_common_prefix(c("11110101101","11110111101")))




set.seed(753)
(bx.p <- boxplot(split(rt(100, 4), gl(5,20))))
op <- par(mfrow= c(2,2))
bxp(bx.p, xaxt = "n")
bxp(bx.p, notch = TRUE, axes = FALSE, pch = 4, boxfill=1:5)
bxp(bx.p, notch = TRUE, boxfill= "lightblue", frame= FALSE, outl= FALSE,
		main = "bxp(*, frame= FALSE, outl= FALSE)")
bxp(bx.p, notch = TRUE, boxfill= "lightblue", border= 2:6, ylim = c(-4,4),
		pch = 22, bg = "green", log = "x", main = "... log='x', ylim=*")
par(op)
op <- par(mfrow= c(1,2))

## single group -- no label
boxplot (weight ~ group, data = PlantGrowth, subset = group=="ctrl")
## with label
bx <- boxplot(weight ~ group, data = PlantGrowth,
		subset = group=="ctrl", plot = FALSE)
bxp(bx,show.names=TRUE)
par(op)

z <- split(rnorm(1000), rpois(1000,2.2))
boxplot(z, whisklty=3, main="boxplot(z, whisklty = 3)")

## Colour support similar to plot.default:
op <- par(mfrow=1:2, bg="light gray", fg="midnight blue")
boxplot(z,   col.axis="skyblue3", main="boxplot(*, col.axis=..,main=..)")
plot(z[[1]], col.axis="skyblue3", main=   "plot(*, col.axis=..,main=..)")
mtext("par(bg=\"light gray\", fg=\"midnight blue\")",
		outer = TRUE, line = -1.2)
par(op)

## Mimic S-Plus:
splus <- list(boxwex=0.4, staplewex=1, outwex=1, boxfill="grey40",
		medlwd=3, medcol="white", whisklty=3, outlty=1, outpch=" ")
boxplot(z, pars=splus)
## Recycled and "sweeping" parameters
op <- par(mfrow=c(1,2))
boxplot(z, border=1:5, lty = 3, medlty = 1, medlwd = 2.5)
boxplot(z, boxfill=1:3, pch=1:5, lwd = 1.5, medcol="white")
par(op)
## too many possibilities
boxplot(z, boxfill= "light gray", outpch = 21:25, outlty = 2,
		bg = "pink", lwd = 2, medcol = "dark blue", medcex = 2, medpch=20)

