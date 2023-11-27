#' Transpose Norwegian list of publication channels
#'
#' @param journals_file path to csv file with data on journals
#' @param publishers_file path to csv file with data on publishers
#' @import dplyr tidyr readr
#' @returns list of 2 tibbles
#' @export
transpose_norwegian_list <- function(journals_file, publishers_file) {

  journals <- read_delim(journals_file,
                         delim = ";",
                         show_col_types = FALSE) |>
    mutate(across(starts_with("Nivå"), as.character)) |>
    dplyr::rename(Title_original = `Original tittel`,
           Title_international = `Internasjonal tittel`,
           issn = `Print ISSN`,
           issn_online = `Online ISSN` )


  suppressWarnings(
    journals_transposed <- journals |>
      select(tidsskrift_id:issn_online, starts_with("Nivå")) |>
      pivot_longer(starts_with("Nivå"),
                   names_to = "Year") |>
      mutate(Year = as.integer(gsub("Nivå ", "", Year)),
             Level = if_else(value %in% c(0, 1, 2), as.integer(value), NA_integer_)) |>
      select(-value)
    )

  publishers <- read_delim(publishers_file,
                           delim = ";",
                           show_col_types = FALSE) |>
    mutate(across(starts_with("Nivå"), as.character)) |>
    dplyr::rename(Title_original = `Original tittel`,
           Title_international = `Internasjonal tittel`,
           isbn_prefix = `ISBN-prefiks`)

  suppressWarnings(
    publishers_transposed <- publishers |>
      select(forlag_id:isbn_prefix, starts_with("Nivå")) |>
      pivot_longer(starts_with("Nivå"),
                   names_to = "Year") |>
      mutate(Year = as.integer(gsub("Nivå ", "", Year)),
             Level = if_else(value %in% c(0, 1, 2), as.integer(value), NA_integer_)) |>
      select(-value)
    )

  list(journals = journals_transposed,
       publishers = publishers_transposed)
}

#' Write csv files norwegian_journals, norwegian_publishers
#'
#' @param data named list of 2 (journals, publishers)
#' @param location a local path or an existing S3 bucket
#' @param to_s3 set to TRUE to write to S3 storage (default FALSE)
#' @importFrom aws.s3 s3write_using
#' @export
write_norwegian_list_csv <- function(data, location, to_s3 = FALSE) {

  if (to_s3) {
    s3write_using(data$journals, write_csv, object = 'norwegian_list_journals.csv')
    s3write_using(data$publishers, write_csv, object = 'norwegian_list_publishers.csv')
  } else {
    write_csv(data$journals, file.path(location, 'norwegian_list_journals.parquet'))
    write_csv(data$publishers, file.path(location, 'norwegian_list_publishers.parquet'))
  }
}


#' Write parquet files norwegian_journals, norwegian_publishers
#'
#' @param data named list of 2 (journals, publishers)
#' @param location a local path or an existing S3 bucket
#' @param to_s3 set to TRUE to write to S3 storage (default FALSE)
#' @importFrom arrow write_parquet
#' @importFrom aws.s3 s3write_using
#' @export
write_norwegian_list_parquet <- function(data, location, to_s3 = FALSE) {

  if (to_s3) {
    s3write_using(data$journals, write_parquet, object = 'norwegian_list_journals.parquet', bucket = location)
    s3write_using(data$publishers, write_parquet, object = 'norwegian_list_publishers.parquet', bucket = location)
  } else {
    write_parquet(data$journals, file.path(location, 'norwegian_list_journals.csv'))
    write_parquet(data$publishers, file.path(location, 'norwegian_list_publishers.csv'))
  }
}
