
# Load a library, or attempt to install it if it's not available
load_library <- function(Name)
  {
    if (!library(Name, character.only = TRUE, logical.return=TRUE))
      {
        install.packages(Name, repos = "http://lib.stat.cmu.edu/R/CRAN")
      }
    print(Name)
  }

# Load a latency file and ensure that it is appropriately tagged
load_latency_frame <- function(File)
  {
    op <- strsplit(basename(File), "_")[[1]][1]
    frame <- read.csv(File)
    frame$op = rep(op, nrow(frame))
    return (frame)
  }

# Load summary and latency information for a given directory
load_benchmark <- function(Dir)
  {
    ## Load up summary data
    summary <- read.csv(sprintf("%s/%s", Dir, "summary.csv"))

    ## Get a list of latency files
    latencies <- lapply(list.files(path = Dir, pattern = "_latencies.csv",
                                   full.names = TRUE),
                        load_latency_frame)
    latencies <- do.call('rbind', latencies)

    ## Convert timing information in latencies from usecs -> msecs
    latencies[4:10] <- latencies[4:10] / 1000

    return (list(summary = summary, latencies = latencies))
  }

load_library("getopt")
load_library("grid")
load_library("ggplot2")
