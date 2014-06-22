args <- commandArgs(trailingOnly = TRUE)

data <- read.csv(file=sprintf("%s.csv", args[1]), header=TRUE, sep=",")

png(sprintf("%s_summary.png", args[1]), width=1024, height=768)

par(mfrow=c(2,2), oma=c(0,1,3,0))

# CPU and memory

plot(data$elapsed, data$cpu, type="l", main="CPU utilization and memory usage", cex.main=1.3, col="red", xlab="Elapsed Secs", ylab="Percentage", ylim=c(0,100), cex.lab=1.1)
lines(data$elapsed, data$mem, type="l", col="blue")
legend("topright", c("CPU","Mem"), col=c("red", "blue"), lty=c(1,1))

# Network transfer

net_rx_b <- rowSums(data[,grep("net_rx_b", colnames(data))]) / length(grep("net_rx_b", colnames(data)))
net_tx_b <- rowSums(data[,grep("net_tx_b", colnames(data))]) / length(grep("net_tx_b", colnames(data)))
max <- max(net_rx_b, net_tx_b)
min <- min(net_rx_b, net_tx_b)

plot(data$elapsed, net_rx_b, type="l", main="Network transfer", cex.main=1.3, col="red", xlab="Elapsed Secs", ylab="Bytes", ylim=c(min,max), cex.lab=1.1)
lines(data$elapsed, net_tx_b, type="l", col="blue")
legend("topright", c("RX", "TX"), col=c("red","blue"), lty=c(1,1))

# Erlang ports transfer

max <- max(data$ports_rx_b, data$ports_tx_b)
min <- min(data$ports_rx_b, data$ports_tx_b)

plot(data$elapsed, data$ports_rx_b, type="l", main="Erlang ports transfer", cex.main=1.3, col="red", xlab="Elapsed Secs", ylab="Bytes", ylim=c(min,max), cex.lab=1.1)
lines(data$elapsed, data$ports_tx_b, type="l", col="blue")
legend("topright", c("RX", "TX"), col=c("red","blue"), lty=c(1,1))

# Network throughput

net_rx_pps <- rowSums(data[,grep("net_rx_pps", colnames(data))]) / length(grep("net_rx_pps", colnames(data)))
net_tx_pps <- rowSums(data[,grep("net_tx_pps", colnames(data))]) / length(grep("net_tx_pps", colnames(data)))
max <- max(net_rx_pps, net_tx_pps)
min <- min(net_rx_pps, net_tx_pps)

plot(data$elapsed, net_rx_pps, type="l", main="Network throughput", cex.main=1.3, col="red", xlab="Elapsed Secs", ylab="Packets / sec", ylim=c(min,max), cex.lab=1.1)
lines(data$elapsed, net_tx_pps, type="l", col="blue")
legend("topright", c("RX", "TX"), col=c("red","blue"), lty=c(1,1))

title(args[2], outer=TRUE, cex.main=1.8)

dev.off()