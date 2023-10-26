load_dw <- function(data_source, table_name) {

  library(rjson)
  library(plyr)
  library(data.table)

  api_url <- paste0("https://mi211storagev2.blob.core.windows.net/curated/",
                    data_source, "/model.json")
  api_parameters <- paste0("?sv=2021-04-10&",
                       "st=2022-12-13T22%3A31%3A08Z&",
                       "se=2023-12-14T22%3A31%3A00Z&",
                       "sr=c&",
                       "sp=rl&",
                       "sig=NLhfbJwdpgIodlsQXN%2Bpiq37REvYCvpD5kjx2qV678s%3D")
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
