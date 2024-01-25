subg <- function(x, pattern, replacement, ...) { gsub(pattern, replacement, x, ...) }

#' Separate delimited columns into rows according to separator
#'
#' @param pubs publication list
#' @param identificator column to separate into rows (default "ISBN")
#' @param keep columns to keep in addition to identificator (default "PID")
#' @param sep the separator to use (default ";")
#' @import tidyr
#' @export
separate_identificator <- function(pubs, identificator = "ISBN", keep = "PID", sep = ";"){
  pubs |>
    select({{keep}}, {{identificator}}) |>
    separate_longer_delim({{identificator}}, delim = sep)
}

isbn_prefix <- function(isbn) { gsub("^([0-9]+-[0-9]+-[0-9]+)(-.*)", "\\1", isbn) }

clean_issn <- function(issn) { trimws(tolower((gsub("[^0-9xX]", "", issn)))) }

# Make journal titles more likely to match
clean_title <- function(title) {

  title |>
    # Switch "&" to "and"
    subg("&amp;", "and") |>
    subg(" & ", " and ") |>
    # Remove trailing volume like chars
    subg("[;:][0-9 ]+$", "") |>
    # Remove tags like "<em>", "<sup>" etc
    subg("<[^>]*>", "") |>
    # Make lowcase and remove leading/trailing whitespace
    tolower() |>
    trimws()
}

# Make conferences more likely to match
clean_conference <- function(conference) {

  date_regex1 <- sprintf("[-0-9 ]* (%s) [0-9]*", paste(month.name, collapse = "|"))
  date_regex2 <- sprintf("(%s) [-0-9, ]*", paste(month.name, collapse = "|"))

  clean_title(conference) |>
    subg("[Pp]roceedings of the ", "") |>
    # Remove dates
    subg(paste0(date_regex1, " (to|through) ", date_regex1), "") |>
    subg(date_regex1, "") |>
    subg(paste0(date_regex2, " (to|through) ", date_regex2), "") |>
    subg(date_regex2, "") |>
    # Remove leading "12th", "XXIV" etc
    subg("^[0-9]+(th|st|nd) ", "") |>
    subg("^[IVX]+ ", "") |>
    # Change some specific chars to space
    subg("[.*/-]", " ") |>
    # Remove non alpha or space
    subg("[^A-Za-z ]", "") |>
    # Remove extra spacing
    subg("[ ]+", " ") |>
    trimws()
}

# Make conferences even more likely to match (also increase risk of false matches of course)
extra_clean_conference <- function(conference) {

  conference |>
    # Remove ", abbrev YYYY" and everything following
    subg(", [A-Za-z]* [0-9]{4}.*", "") |>
    # Remove "(whatever)"
    subg(" \\([A-Za-z 0-9]*\\).*", "") |>
    # Remove "International"
    subg("[Ii]nternational ", "") |>
    # Remove IEEE
    subg("^(IEEE|ieee) ", "") |>
    subg(" (IEEE|ieee) ", " ") |>
    clean_conference()
}

