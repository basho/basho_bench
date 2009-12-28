library(lattice)

png(file = "results.png", width = 1024, height = 768)

# First plot req/sec from summary.csv; drop first 5 mins (300 secs)
summary <- read.csv("summary.csv")
#summary <- summary[summary$elapsed >= 300,]
plot1 <- xyplot((total / window) ~ elapsed, data = summary, type="l",
                xlab = "Elapsed Secs", ylab = "Requests/sec",
                main = "Throughput")

# Global var that will hold stacked data frames
# from latency files
Latency = NULL

# Function to load a given latency data frame and merge
# with global
load_latency_frame <- function(File)
{
  Op <- strsplit(File, "_")[[1]][1]
  FileFrame <- read.csv(File)
  Frame = data.frame(FileFrame, rep(Op, nrow(FileFrame)))
  names(Frame)[length(Frame)] = "op"
  Latency <<- rbind(Latency, Frame)
}
  
# Get list of latency files and identify the individual operations
LatencyFiles <- list.files(pattern = "_latencies.csv")
LatencyOps <- unlist(strsplit(LatencyFiles, "_"))[seq(1,length(LatencyFiles)*2,2)]

# Load each latency file and build single frame
lapply(LatencyFiles, load_latency_frame)

# Scale all timing information to msecs (from usecs)
Latency[4:10] <- Latency[4:10] / 1000

# Drop first 5 mins (300 secs) of test data
#Latency <- Latency[Latency$elapsed >= 300,]

plot2 <- xyplot(X95th + X99th + X99_9th + max~ elapsed | op, data = Latency,
                layout = c(length(LatencyOps), 1),
                xlab = "Elapsed Secs",
                ylab = "99th, 99.9th, max (ms)",
                main = "Latency")

plot3 <- xyplot(median + mean + X95th  ~ elapsed | op, data = Latency,
                layout = c(length(LatencyOps), 1),
                xlab = "Elapsed Secs",
                ylab = "Median, Mean, 95th (ms)",
                main = "Latency")

print(plot1, position = c(0, 0.66, 1, 1), more = TRUE)
print(plot2, position = c(0, 0.33, 1, 0.66), more = TRUE)
print(plot3, position = c(0, 0, 1, 0.33))
dev.off()

