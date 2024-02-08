load_dw <- function(data_source, table_name) {

  library(rjson)
  library(plyr)
  library(data.table)

  api_url <- paste0("https://mi211storagev2.blob.core.windows.net/curated/",
                    data_source, "/model.json")
  api_parameters <- paste0("?sv=2023-01-03&",
                       "st=2024-01-03T15%3A10%3A51Z&",
                       "se=2025-12-04T15%3A10%3A00Z&",
                       "sr=c&",
                       "sp=rl&",
                       "sig=5qH5R6HSZ6LKhywKKG%2BToT9Rs%2FL6aZrmMOdQQlYBDaY%3D")
  model <- fromJSON(file = paste0(api_url, api_parameters))

  table_names <- sapply(model$entities, function(x) x$name)

  selected_id <- which(table_names == table_name)
  partition_count <- length(model['entities'][[1]][[selected_id]]['partitions'][[1]])

  dw_table <- as.data.frame(fread(paste0(model['entities'][[1]][[selected_id]]['partitions'][[1]][[1]]['location'][[1]], # Initialize table
                              api_parameters), colClasses = 'character'))

  for (j in 2:partition_count) {

    partition <- as.data.frame(fread(paste0(model['entities'][[1]][[selected_id]]['partitions'][[1]][[j]]['location'][[1]], # Load data in each partition
                                 api_parameters), colClasses = 'character'))

    dw_table <- rbind.fill(dw_table, partition)

  }

  return(dw_table)

}
