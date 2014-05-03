library(abind)

# Setup functions (mean, sum, etc) per column name
compute = NULL
compute$mean        = function(x) { mean(x) }
compute$sum         = function(x) { sum(x) }
compute$none        = function(x) { x[1] }
compute$default     = compute$mean
compute$elapsed     = compute$none
compute$window      = compute$none

compute$total       = compute$sum
compute$successful  = compute$sum
compute$failed      = compute$sum
compute$errors      = compute$sum

# Columns to average when combining (left for default):
# n, min, mean, median, 95th, 99th, 99_9th, max, errors


# Make results directory
dir.create(file.path("./", "combined"), showWarnings = FALSE)

# Find all 'testX' directories in current directory
list_dirs <- list.files("./", pattern = "test[0-9][0-9]?")

# Peek to find the set of CSV files
files <- list.files(paste(list_dirs[[1]], "/tests/current/", sep=""), pattern = "csv")

combine_filetype = function(file_name){

    print(paste("Compiling", file_name))

    # Read csv files as an array of matrices
    list_data <- lapply(list_dirs, function(x) {
        file <- paste(x, "/tests/current/",file_name, sep="")
        df <- read.csv(file)
        data.matrix(df)
    })

    # Combine the list of matrices to a 3d matrix
    data <- abind(list_data, along=3)

    # For each column name...
    new_data <- sapply(colnames(data), function(name) {
        fun = NULL
        # find the compute function defined, or use default
        if (is.null(compute[[name]])) {
            paste("Using default fun for", name)
            fun = compute$default
        } else
            fun = compute[[name]]
        # and apply the function to the column of data
        apply(data[,name,],1,fun)
    })

    # write combined results
    write.csv(new_data, file=paste("./combined/", file_name, sep=""), row.names = FALSE)

}

# for each *.csv file type, combine
res <- sapply(files, combine_filetype)

#combine_filetype("short.csv")