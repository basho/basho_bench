#!/usr/bin/env Rscript --vanilla

# Parse the --file= argument out of command line args and
# determine where base directory is so that we can source
# our common sub-routines
arg0 <- sub("--file=(.*)", "\\1", grep("--file=", commandArgs(), value = TRUE))
dir0 <- dirname(arg0)
source(file.path(dir0, "common.r"))

# Setup parameters for the script
params = matrix(c(
  'help',    'h', 0, "logical",
  'width',   'x', 2, "integer",
  'height',  'y', 2, "integer",
  'outfile', 'o', 2, "character",
  'indir',   'i', 2, "character",
  'tstart',  '1',  2, "integer",
  'tend',    '2',  2, "integer"
  ), ncol=4, byrow=TRUE)

# Parse the parameters
opt = getopt(params)

if (!is.null(opt$help))
  {
    cat(paste(getopt(params, command = basename(arg0), usage = TRUE)))
    q(status=1)
  }

# Initialize defaults for opt
if (is.null(opt$width))   { opt$width   = 1024 }
if (is.null(opt$height))  { opt$height  = 768 }
if (is.null(opt$indir))   { opt$indir  = "current"}
if (is.null(opt$outfile)) { opt$outfile = file.path(opt$indir, "summary.png") }

# Load the benchmark data, passing the time-index range we're interested in
b = load_benchmark(opt$indir, opt$tstart, opt$tend)

# If there is no actual data available, bail
if (nrow(b$latencies) == 0)
{
  stop("No latency information available to analyze in ", opt$indir)
}

png(file = opt$outfile, width = opt$width, height = opt$height)

# First plot req/sec from summary
plot1 <- qplot(elapsed, successful / window, data = b$summary,
                geom = c("smooth", "point"),
                xlab = "Elapsed Secs", ylab = "Op/sec",
                main = "Throughput") +
                geom_smooth(aes(y = failed / window, colour = "Errors")) +
                scale_colour_manual(name = "", values = c("red"))

# Setup common elements of the latency plots
latency_plot <- ggplot(b$latencies, aes(x = elapsed)) +
                   facet_grid(. ~ op) +
                   labs(x = "Elapsed Secs", y = "Latency (ms)")

# Plot 99 and 99.9th percentiles
plot2 <- latency_plot +
            geom_smooth(aes(y = X99th, color = "X99th")) +
            geom_smooth(aes(y = X99_9th, color = "X99_9th")) +
            scale_color_hue("Percentile",
                            breaks = c("X99th", "X99_9th"),
                            labels = c("99th", "99.9th"))


# Plot median, mean and 95th percentiles
plot3 <- latency_plot +
            geom_smooth(aes(y = median, color = "median")) +
            geom_smooth(aes(y = mean, color = "mean")) +
            geom_smooth(aes(y = X95th, color = "X95th")) +
            scale_color_hue("Percentile",
                            breaks = c("median", "mean", "X95th"),
                            labels = c("Median", "Mean", "95th"))

grid.newpage()

pushViewport(viewport(layout = grid.layout(3, 1)))

vplayout <- function(x,y) viewport(layout.pos.row = x, layout.pos.col = y)

print(plot1, vp = vplayout(1,1))
print(plot2, vp = vplayout(2,1))
print(plot3, vp = vplayout(3,1))

dev.off()
