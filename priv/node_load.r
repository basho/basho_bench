args <- commandArgs(trailingOnly = TRUE)

data <- read.csv(file=sprintf("%s.csv", args[1]), header=TRUE, sep=",")

png(sprintf("%s.png", args[1]), width=1024, height=768)

par(mfrow=c(2,1))
par(mar=c(3, 3, 3, 10), xpd=TRUE)

D <- 50
cpu_percent <- 100 * (1 - D / (D + data$cpu * 256))

plot(spline(data$elapsed, cpu_percent, method="natural"), type="l", main=args[2], col="red", xlab="Elapsed Secs", ylab="Load", ylim=c(0,100))
lines(spline(data$elapsed, data$mem * 100, method="natural"), type="l", col="blue")
legend("topright", c("CPU","Memory"), col=c("red", "blue"), lty=c(1,1), inset=c(-0.14,0.4), title="Legend")

max=max(data$input, data$output)
min=min(data$input, data$output)

plot(spline(data$elapsed, data$input, method="natural"), type="l", main=args[2], col="red", xlab="Elapsed Secs", ylab="Packets", ylim=c(min,max))
lines(spline(data$elapsed, data$output, method="natural"), type="l", col="blue")
legend("topright", c("RX bytes", "TX bytes"), col=c("red","blue"), lty=c(1,1), , inset=c(-0.15,0.4), title="Legend", cex=1.1)

dev.off()
