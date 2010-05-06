#!/usr/bin/env Rscript --vanilla

source("priv/common.r")

# Setup parameters for the script
params = matrix(c(
  'width',   'w', 2, "integer",
  'height',  'h', 2, "integer",
  'outfile', 'o', 2, "character",
  'indir',   'i', 2, "character"
  ), ncol=4, byrow=TRUE)

# Parse the parameters
opt = getopt(params)

# Initialize defaults for opt
if (is.null(opt$width))   { opt$width   = 1024 }
if (is.null(opt$height))  { opt$height  = 768 }
if (is.null(opt$outfile)) { opt$outfile = "summary.png" }
if (is.null(opt$indir))   { opt$indir  = "current"}

# Load the benchmark data
b = load_benchmark(opt$indir)

# If there is no actual data available, bail
if (nrow(b$latencies) == 0)
{
  stop("No latency information available to analyze in ", opt$indir)
}

png(file = opt$outfile, width = opt$width, height = opt$height)

# Construct a plot of latencies, faceted by operation. The ...
# signifies a list variables which provide a printable name
# for the given variable.
latency_plot <- function(benchmark, ...)
  {
    vars <- list(...)
    return (qplot(elapsed, value,
                  data = melted_latency(benchmark, names(vars)),
                  geom = "smooth",
                  facets = . ~ op,
                  color = variable,
                  xlab = "Elapsed Secs",
                  ylab = "",
                  main = "Latency (ms)") +
            scale_color_hue("",
                            breaks = names(vars),
                            labels = unlist(vars, use.names = FALSE)))
  }


# First plot req/sec from summary
plot1 <- qplot(elapsed, total / window, data = b$summary,
               geom = "smooth",
               xlab = "Elapsed Secs", ylab = "Req/sec",
               main = "Throughput")

# Break out latency plots
plot2 <- latency_plot(b, X99th = "99th", X99_9th = "99.9th")
plot3 <- latency_plot(b, median = "Median", mean = "Mean", X95th = "95th")

grid.newpage()

pushViewport(viewport(layout = grid.layout(3, 1)))

vplayout <- function(x,y) viewport(layout.pos.row = x, layout.pos.col = y)

print(plot1, vp = vplayout(1,1))
print(plot2, vp = vplayout(2,1))
print(plot3, vp = vplayout(3,1))

dev.off()

