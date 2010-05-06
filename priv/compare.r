#!/usr/bin/env Rscript --vanilla

source("priv/common.r")

# Setup parameters for the script
params = matrix(c(
  'width',   'w', 2, "integer",
  'height',  'h', 2, "integer",
  'outfile', 'o', 2, "character",
  'dir1',    'i', 1, "character",
  'tag1',    'j', 1, "character",
  'dir2',    'k', 1, "character",
  'tag2',    'l', 1, "character"
  ), ncol=4, byrow=TRUE)

# Parse the parameters
opt = getopt(params)

# Initialize defaults for opt
if (is.null(opt$width))   { opt$width   = 1024 }
if (is.null(opt$height))  { opt$height  = 768 }
if (is.null(opt$outfile)) { opt$outfile = "compare.png" }

# Load the benchmark data for each directory
b1 = load_benchmark(opt$dir1)
b2 = load_benchmark(opt$dir2)

# If there is no actual data available, bail
if (nrow(b1$latencies) == 0)
{
  stop("No latency information available to analyze in ", opt$indir)
}

if (nrow(b2$latencies) == 0)
{
  stop("No latency information available to analyze in ", opt$indir)
}

png(file = opt$outfile, width = opt$width, height = opt$height)

# Tag the summary frames for each benchmark so that we can distinguish
# between them in the legend.
b1$summary$tag <- opt$tag1
b2$summary$tag <- opt$tag2

# Compare the req/sec between the two datasets
plot1 <- qplot(elapsed, total / window,
               data = b1$summary,
               color = tag,
               geom = "smooth",
               xlab = "Elapsed Secs",
               ylab = "Req/sec",
               main = "Throughput") + geom_smooth(data = b2$summary)


grid.newpage()

pushViewport(viewport(layout = grid.layout(3, 1)))

vplayout <- function(x,y) viewport(layout.pos.row = x, layout.pos.col = y)

print(plot1, vp = vplayout(1,1))
#print(plot2, vp = vplayout(2,1))
#print(plot3, vp = vplayout(3,1))

dev.off()

