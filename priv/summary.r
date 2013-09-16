#!/usr/bin/env Rscript --vanilla

# Parse the --file= argument out of command line args and
# determine where base directory is so that we can source
# our common sub-routines
arg0 <- sub("--file=(.*)", "\\1", grep("--file=", commandArgs(), value = TRUE))
dir0 <- dirname(arg0)
source(file.path(dir0, "common.r"))

theme_set(theme_grey(base_size = 17)) 

# Setup parameters for the script
params = matrix(c(
  'help',    'h', 0, "logical",
  'width',   'x', 2, "integer",
  'height',  'y', 2, "integer",
  'outfile', 'o', 2, "character",
  'indir',   'i', 2, "character",
  'tstart',  '1',  2, "integer",
  'tend',    '2',  2, "integer",
  'ylabel1stgraph', 'Y',  2, "character"
  ), ncol=4, byrow=TRUE)

# Parse the parameters
opt = getopt(params)

if (!is.null(opt$help))
  {
    cat(paste(getopt(params, command = basename(arg0), usage = TRUE)))
    q(status=1)
  }

# Initialize defaults for opt
if (is.null(opt$width))   { opt$width   = 1280 }
if (is.null(opt$height))  { opt$height  = 960 }
if (is.null(opt$indir))   { opt$indir  = "current"}
if (is.null(opt$outfile)) { opt$outfile = file.path(opt$indir, "summary.png") }
if (is.null(opt$ylabel1stgraph)) { opt$ylabel1stgraph = "Op/sec" }

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
                xlab = "Elapsed Secs", ylab = opt$ylabel1stgraph,
                main = "Throughput") +
              
                geom_smooth(aes(y = successful / window, colour = "ok"), size=0.5) +
                geom_point(aes(y = successful / window, colour = "ok"), size=2.0) +
                
                geom_smooth(aes(y = failed / window, colour = "error"), size=0.5) +
                geom_point(aes(y = failed / window, colour = "error"), size=2.0) +

                scale_colour_manual("Response", values = c("#FF665F", "#188125"))


# Setup common elements of the latency plots
latency_plot <- ggplot(b$latencies, aes(x = elapsed)) +
                   facet_grid(. ~ op) +
                   labs(x = "Elapsed Secs", y = "Latency (ms)")

# Plot 99 and 99.9th percentiles
plot2 <- latency_plot +
            geom_smooth(aes(y = X99th, color = "99th"), size=0.5) +
            geom_point(aes(y = X99th, color = "99th"), size=2.0) +

            geom_smooth(aes(y = X99_9th, color = "99.9th"), size=0.5) +
            geom_point(aes(y = X99_9th, color = "99.9th"), size=2.0) +
            
            scale_colour_manual("Percentile", values = c("#FF665F", "#009D91"))
            # scale_color_hue("Percentile",
            #                 breaks = c("X99_9th","X99th" ),
            #                 labels = c("99.9th", "99th"))


# Plot median, mean and 95th percentiles
plot3 <- latency_plot +
            geom_smooth(aes(y = median, color = "median"), size=0.5) +
            geom_point(aes(y = median, color = "median"), size=2.0) +

            geom_smooth(aes(y = mean, color = "mean"), size=0.5) +
            geom_point(aes(y = mean, color = "mean"), size=2.0) +

            geom_smooth(aes(y = X95th, color = "95th"), size=0.5) +
            geom_point(aes(y = X95th, color = "95th"), size=2.0) +

            scale_colour_manual("Percentile", values = c("#FF665F", "#009D91", "#FFA700"))
            # scale_color_hue("Percentile",
            #                 breaks = c("X95th", "mean", "median"),
            #                 labels = c("95th", "Mean", "Median"))

grid.newpage()

pushViewport(viewport(layout = grid.layout(3, 1)))

vplayout <- function(x,y) viewport(layout.pos.row = x, layout.pos.col = y)

print(plot1, vp = vplayout(1,1))
print(plot2, vp = vplayout(2,1))
print(plot3, vp = vplayout(3,1))

dev.off()
