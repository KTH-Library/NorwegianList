#' Transpose Norwegian journals list
#'
#' @param journals_file path to csv file with data on journals
#' @import dplyr tidyr readr
#' @returns tibble
#' @export
transpose_journals_list <- function(journals_file) {

  colnames <- read_delim(journals_file, col_names = F, n_max = 1, delim = ";", show_col_types = FALSE) |> as.character()

  readcols <- colnames[which(colnames %in% c("tidsskrift_id",
                                             "Original tittel",
                                             "Internasjonal tittel",
                                             "Print ISSN",
                                             "Online ISSN") |
                               grepl("Niv\u00E5", colnames))]


  coltypes <- paste(c("i", rep("c", length(colnames)-1)), collapse = "")

  read_delim(journals_file,
             delim = ";",
             trim_ws = TRUE,
             col_select = all_of(readcols),
             col_types = coltypes) |>
    pivot_longer(starts_with("Niv\u00E5"),
                 names_to = "Year") |>
    mutate(Year = as.integer(gsub("Niv\u00E5 ", "", Year))) |>
    rename(Title_original = `Original tittel`,
           Title_international = `Internasjonal tittel`,
           issn = `Print ISSN`,
           issn_online = `Online ISSN`,
           Level = value)
}

#' Transpose Norwegian publishers list
#'
#' @param publishers_file path to csv file with data on journals
#' @import dplyr tidyr readr
#' @returns tibble
#' @export
transpose_publishers_list <- function(publishers_file) {

  colnames <- read_delim(publishers_file, col_names = F, n_max = 1, delim = ";", show_col_types = FALSE) |> as.character()

  readcols <- colnames[which(colnames %in% c("forlag_id",
                                             "Original tittel",
                                             "Internasjonal tittel",
                                             "ISBN-prefiks") |
                               grepl("Niv\u00E5", colnames))]

  coltypes <- paste(c("i", rep("c", length(colnames)-1)), collapse = "")

  read_delim(publishers_file,
             delim = ";",
             trim_ws = TRUE,
             col_select = all_of(readcols),
             col_types = coltypes) |>
    pivot_longer(starts_with("Niv\u00E5"),
                 names_to = "Year") |>
    mutate(Year = as.integer(gsub("Niv\u00E5 ", "", Year))) |>
    rename(Title_original = `Original tittel`,
           Title_international = `Internasjonal tittel`,
           isbn_prefix = `ISBN-prefiks`,
           Level = value)
}

#' Transpose Norwegian lists of publication channels
#'
#' @param journals_file path to csv file with data on journals
#' @param publishers_file path to csv file with data on publishers
#' @import dplyr tidyr readr
#' @returns list of 2 tibbles
#' @export
transpose_norwegian_list <- function(journals_file, publishers_file) {

  list(journals = transpose_journals_list(journals_file),
       publishers =  transpose_publishers_list(publishers_file))
}

#' Write csv files norwegian_journals, norwegian_publishers
#'
#' @param data named list of 2 (journals, publishers)
#' @param location a local path or an existing S3 bucket
#' @param to_s3 set to TRUE to write to S3 storage (default FALSE)
#' @importFrom aws.s3 s3write_using
#' @importFrom readr write_csv
#' @export
write_norwegian_list_csv <- function(data, location, to_s3 = FALSE) {

  if (to_s3) {
    s3write_using(data$journals, write_csv, object = 'norwegian_list_journals.csv', bucket = location)
    s3write_using(data$publishers, write_csv, object = 'norwegian_list_publishers.csv', bucket = location)
  } else {
    write_csv(data$journals, file.path(location, 'norwegian_list_journals.csv'))
    write_csv(data$publishers, file.path(location, 'norwegian_list_publishers.csv'))
  }
}


#' Write parquet files norwegian_journals, norwegian_publishers
#'
#' @param data named list of 2 (journals, publishers)
#' @param location a local path or an existing S3 bucket
#' @param to_s3 set to TRUE to write to S3 storage (default FALSE)
#' @importFrom aws.s3 s3write_using
#' @importFrom arrow write_parquet
#' @export
write_norwegian_list_parquet <- function(data, location, to_s3 = FALSE) {

  if (to_s3) {
    s3write_using(data$journals, write_parquet, object = 'norwegian_list_journals.parquet', bucket = location)
    s3write_using(data$publishers, write_parquet, object = 'norwegian_list_publishers.parquet', bucket = location)
  } else {
    write_parquet(data$journals, file.path(location, 'norwegian_list_journals.parquet'))
    write_parquet(data$publishers, file.path(location, 'norwegian_list_publishers.parquet'))
  }
}

#' Read csv files norwegian_journals, norwegian_publishers
#'
#' @param location a local path or an existing S3 bucket
#' @param from_s3 set to TRUE to read from S3 storage (default FALSE)
#' @importFrom aws.s3 s3read_using
#' @importFrom readr read_csv
#' @export
read_norwegian_list_csv <- function(location, from_s3 = FALSE) {

  if (from_s3) {
    journals <- s3read_using(read_csv, object = 'norwegian_list_journals.csv', bucket = location)
    publishers <- s3read_using(read_csv, object = 'norwegian_list_publishers.csv', bucket = location)
  } else {
    journals <- read_csv(file.path(location, 'norwegian_list_journals.csv'))
    publishers <- read_csv(file.path(location, 'norwegian_list_publishers.csv'))
  }
  list(journals = journals,
       publishers = publishers)
}


#' Read parquet files norwegian_journals, norwegian_publishers
#'
#' @param location a local path or an existing S3 bucket
#' @param from_s3 set to TRUE to read from S3 storage (default FALSE)
#' @importFrom aws.s3 s3write_using
#' @importFrom arrow read_parquet
#' @export
read_norwegian_list_parquet <- function(location, from_s3 = FALSE) {

  if (from_s3) {
    journals <- s3read_using(read_parquet, object = 'norwegian_list_journals.parquet', bucket = location)
    publishers <- s3read_using(read_parquet, object = 'norwegian_list_publishers.parquet', bucket = location)
  } else {
    journals <- read_parquet(file.path(location, 'norwegian_list_journals.parquet'))
    publishers <- read_parquet(file.path(location, 'norwegian_list_publishers.parquet'))
  }
  list(journals = journals,
       publishers = publishers)
}
