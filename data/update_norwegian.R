# Update norwegian list csv and parquet files at kthb/bibliometrics S3 bucket
# First download files from https://kanalregister.hkdir.no/informasjonsartikler/last-ned-gjeldande-liste

library(NorwegianList)
library(tidyverse)

nordir <- "" # Set to directory where raw csv files are

setwd(nordir)

# Extract the latest journal and publisher files
filer <- list.files() |>
  data.frame() |>
  setNames("filename") |>
  arrange(filename)

journal_file <- filer |>
  filter(grepl("tidsskrifter", filename)) |>
  tail(1) |>
  pull(filename)

publisher_file <- filer |>
  filter(grepl("forlag", filename)) |>
  tail(1) |>
  pull(filename)

# Transpose both datasets
transponerad <- transpose_norwegian_list(journal_file, publisher_file)

# Save transposed data to csv and parquet
write_norwegian_list_csv(transponerad, 'bibliometrics', to_s3 = TRUE)
write_norwegian_list_parquet(transponerad, 'bibliometrics', to_s3 = TRUE)
