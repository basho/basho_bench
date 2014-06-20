args <- commandArgs(trailingOnly = TRUE)

data <- read.csv(file=sprintf("%s.csv", args[1]), header=TRUE, sep=",")

png(sprintf("%s.png", args[1]), width=1024, height=768)

par(mfrow=c(2,2), oma=c(0,1,3,0))

# CPU and memory

columns <- grep("core", colnames(data))
cores <- data[,columns]
length <- length(columns)
colours <- rainbow(length + 2)
columnNames <- gsub("core", "Core", colnames(cores))

plot(data$elapsed, data$cpu, type="l", main="CPU utilization and memory usage", cex.main=1.3, col=colours[1], xlab="Elapsed Secs", ylab="Percentage", ylim=c(0,100), cex.lab=1.1, lwd=3)
if(length > 0) {
	for(i in 1:length) {
		lines(data$elapsed, cores[,i], type="l", col=colours[i+1])
	}
}
lines(data$elapsed, data$mem, type="l", col=colours[length + 2], lwd=3)
legend("topright", c("CPU", columnNames, "Mem"), col=colours, lty=rep.int(1, length + 2))

# Network transfer

columns <- grep("net_[rt]x_b", colnames(data))
net <- data[,columns]
max <- max(net)
min <- min(net)
length <- length(columns)
colours <- rainbow(length)
columnNames <- gsub("net_rx_b_", "RX ", colnames(net))
columnNames <- gsub("net_tx_b_", "TX ", columnNames)

plot(data$elapsed, net[,1], type="l", main="Network transfer", cex.main=1.3, col=colours[1], xlab="Elapsed Secs", ylab="Bytes", ylim=c(min,max), cex.lab=1.1)
for(i in 2:length) {
	lines(data$elapsed, net[,i], type="l", col=colours[i])
}
legend("topright", columnNames, col=colours, lty=rep.int(1, length))

# Erlang ports transfer

max <- max(data$ports_rx_b, data$ports_tx_b)
min <- min(data$ports_rx_b, data$ports_tx_b)

plot(data$elapsed, data$ports_rx_b, type="l", main="Erlang ports transfer", cex.main=1.3, col="red", xlab="Elapsed Secs", ylab="Bytes", ylim=c(min,max), cex.lab=1.1)
lines(data$elapsed, data$ports_tx_b, type="l", col="blue")
legend("topright", c("RX", "TX"), col=c("red","blue"), lty=c(1,1))

# Network throughput

columns <- grep("net_[rt]x_pps", colnames(data))
net <- data[,columns]
max <- max(net)
min <- min(net)
length <- length(columns)
colours <- rainbow(length)
columnNames <- gsub("net_rx_pps_", "RX ", colnames(net))
columnNames <- gsub("net_tx_pps_", "TX ", columnNames)

plot(data$elapsed, net[,1], type="l", main="Network throughput", cex.main=1.3, col=colours[1], xlab="Elapsed Secs", ylab="Packets / sec", ylim=c(min,max), cex.lab=1.1)
for(i in 2:length) {
	lines(data$elapsed, net[,i], type="l", col=colours[i])
}
legend("topright", columnNames, col=colours, lty=rep.int(1, length))

title(args[2], outer=TRUE, cex.main=1.8)

dev.off()