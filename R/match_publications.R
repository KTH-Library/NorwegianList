#' Match DiVA publications with Norwegian list on closest year
#'
#' @param pubs publication list to match
#' @param norlist Norwegian list to match
#' @param matchpubs field to match on from pubs, default issn
#' @import dplyr
#' @param matchlist field to match on from list, default issn
get_closest <- function(pubs, norlist, matchpubs = "issn", matchlist = "issn") {

  pubs_work <- pubs |>
    rename(matchvar = all_of(matchpubs),
           pub_year = Year) |>
    select(PID, pub_year, matchvar) |>
    filter(!is.na(matchvar))

  list_work <- norlist |>
    rename(matchvar = all_of(matchlist),
           list_year = Year) |>
    filter(!is.na(Level), !is.na(matchvar))

  pubs_work |>
    inner_join(list_work, by = "matchvar", relationship = "many-to-many") |>
    mutate(diff_year = if_else(list_year > pub_year,
                               list_year - pub_year + 0.5, # Add 0.5 to prefer earlier match if equal distance
                               pub_year - list_year)) |>
    group_by(PID) |>
    mutate(rank = rank(desc(diff_year))) |>
    filter(rank == 1) |>
    select(PID, Level, list_year) |>
    ungroup()
}

#' Match DiVA publications with Norwegian List journals by issn
#'
#' @param journals list of journal levels by year
#' @param publications publication list from DiVA
#' @param closest_if_missing set to TRUE to try closest year if level is missing
#' @import dplyr
#' @export
match_diva_issn <- function(journals, publications, closest_if_missing = FALSE){

  journals_match <- journals |>
    mutate(issn = clean_issn(issn),
           eissn = clean_issn(issn_online))

  pubs_match <- publications |>
    filter(!is.na(coalesce(JournalISSN, JournalEISSN, SeriesISSN, SeriesEISSN))) |>
    mutate(issn = clean_issn(JournalISSN),
           eissn = clean_issn(JournalEISSN),
           sissn = clean_issn(SeriesISSN),
           seissn = clean_issn(SeriesEISSN))

  pids <- pubs_match$PID

  issn_issn <- pubs_match |>
    filter(!is.na(issn)) |>
    inner_join(journals_match, by = c("issn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- issn_issn |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(issn %in% pubs_na$issn)
    closest <- get_closest(pubs_na, journals_na, "issn", "issn")
    issn_issn <- issn_issn |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  issn_issn <- issn_issn |>
    filter(!is.na(Level))|>
    mutate(matched = "JournalISSN")

  pids <- setdiff(pids, issn_issn$PID)

  issn_issne <- pubs_match |>
    filter(!is.na(issn), PID %in% pids) |>
    inner_join(journals_match, by = c("issn" = "eissn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- issn_issne |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(eissn %in% pubs_na$issn)
    closest <- get_closest(pubs_na, journals_na, "issn", "eissn")
    issn_issne <- issn_issne |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  issn_issne <- issn_issne |>
    filter(!is.na(Level)) |>
    mutate(matched = "JournalISSN")

  pids <- setdiff(pids, issn_issne$PID)

  issne_issne <- pubs_match |>
    filter(!is.na(eissn), PID %in% pids) |>
    inner_join(journals_match, by = c("eissn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- issne_issne |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(eissn %in% pubs_na$eissn)
    closest <- get_closest(pubs_na, journals_na, "eissn", "eissn")
    issne_issne <- issne_issne |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  issne_issne <- issne_issne |>
    filter(!is.na(Level)) |>
    mutate(matched = "JournalEISSN")

  pids <- setdiff(pids, issne_issne$PID)

  issne_issn <- pubs_match |>
    filter(!is.na(eissn), PID %in% pids) |>
    inner_join(journals_match, by = c("eissn" = "issn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- issne_issn |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(issn %in% pubs_na$eissn)
    closest <- get_closest(pubs_na, journals_na, "eissn", "issn")
    issne_issn <- issne_issn |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  issne_issn <- issne_issn |>
    filter(!is.na(Level)) |>
    mutate(matched = "JournalEISSN")

  pids <- setdiff(pids, issne_issn$PID)

  sissn_issn <- pubs_match |>
    filter(!is.na(sissn), PID %in% pids) |>
    inner_join(journals_match, by = c("sissn" = "issn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- sissn_issn |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(issn %in% pubs_na$sissn)
    closest <- get_closest(pubs_na, journals_na, "sissn", "issn")
    sissn_issn <- sissn_issn |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  sissn_issn <- sissn_issn |>
    filter(!is.na(Level)) |>
    mutate(matched = "SeriesISSN")

  pids <- setdiff(pids, sissn_issn$PID)

  sissn_issne <- pubs_match |>
    filter(!is.na(sissn), PID %in% pids) |>
    inner_join(journals_match, by = c("sissn" = "eissn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- sissn_issne |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(eissn %in% pubs_na$sissn)
    closest <- get_closest(pubs_na, journals_na, "sissn", "eissn")
    sissn_issne <- sissn_issne |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  sissn_issne <- sissn_issne |>
    filter(!is.na(Level)) |>
    mutate(matched = "SeriesISSN")

  pids <- setdiff(pids, sissn_issne$PID)

  sissne_issne <- pubs_match |>
    filter(!is.na(seissn), PID %in% pids) |>
    inner_join(journals_match, by = c("seissn" = "eissn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- sissne_issne |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(eissn %in% pubs_na$seissn)
    closest <- get_closest(pubs_na, journals_na, "seissn", "eissn")
    sissne_issne <- sissne_issne |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  sissne_issne <- sissne_issne |>
    filter(!is.na(Level)) |>
    mutate(matched = "SeriesEISSN")

  pids <- setdiff(pids, sissne_issne$PID)

  sissne_issn <- pubs_match |>
    filter(!is.na(seissn), PID %in% pids) |>
    inner_join(journals_match, by = c("seissn" = "issn", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- sissne_issn |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(issn %in% pubs_na$seissn)
    closest <- get_closest(pubs_na, journals_na, "seissn", "issn")
    issne_issn <- issne_issn |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  sissne_issn <- sissne_issn |>
    filter(!is.na(Level)) |>
    mutate(matched = "SeriesEISSN")

  bind_rows(issn_issn, issn_issne, issne_issne, issne_issn,
            sissn_issn, sissn_issne, sissne_issne, sissne_issn) |>
    distinct()
}

#' Match DiVA publications with Norwegian List journals by title
#'
#' @param journals list of journal levels by year
#' @param publications publication list from DiVA
#' @param closest_if_missing set to TRUE to try closest year if level is missing
#' @import dplyr
#' @export
match_diva_journal <- function(journals, publications, closest_if_missing = FALSE){

  journals_match <- journals |>
    mutate(title = clean_title(Title_original),
           title_int = clean_title(Title_international))

  pubs_match <- publications |>
    filter(!is.na(coalesce(Journal, Series, HostPublication))) |>
    mutate(title = clean_title(Journal),
           stitle = clean_title(Series),
           htitle = clean_title(HostPublication))

  pids <- pubs_match$PID

  title_title <- pubs_match |>
    filter(!is.na(title)) |>
    inner_join(journals_match, by = c("title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- title_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$title)
    closest <- get_closest(pubs_na, journals_na, "title", "title")
    title_title <- title_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  title_title <- title_title |>
    filter(!is.na(Level)) |>
    mutate(matched = "Journal")

  pids <- setdiff(pids, title_title$PID)

  title_int <- pubs_match |>
    filter(!is.na(title), PID %in% pids) |>
    inner_join(journals_match, by = c("title" = "title_int", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- title_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$title)
    closest <- get_closest(pubs_na, journals_na, "title", "title_int")
    title_int <- title_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  title_int <- title_int |>
    filter(!is.na(Level)) |>
    mutate(matched = "Journal")

  pids <- setdiff(pids, title_int$PID)

  series_title <- pubs_match |>
    filter(!is.na(stitle), PID %in% pids) |>
    inner_join(journals_match, by = c("stitle" = "title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$stitle)
    closest <- get_closest(pubs_na, journals_na, "stitle", "title")
    series_title <- series_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_title <- series_title |>
    filter(!is.na(Level)) |>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_title$PID)

  series_int <- pubs_match |>
    filter(!is.na(stitle), PID %in% pids) |>
    inner_join(journals_match, by = c("stitle" = "title_int", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$stitle)
    closest <- get_closest(pubs_na, journals_na, "stitle", "title_int")
      series_int <- series_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_int <- series_int |>
    filter(!is.na(Level)) |>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_int$PID)

  host_title <- pubs_match |>
    filter(!is.na(htitle), PID %in% pids) |>
    inner_join(journals_match, by = c("htitle" = "title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- host_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$htitle)
    closest <- get_closest(pubs_na, journals_na, "htitle", "title")
    host_title <- host_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_title <- host_title |>
    filter(!is.na(Level)) |>
    mutate(matched = "HostPublication")

  pids <- setdiff(pids, host_title$PID)

  host_int <- pubs_match |>
    filter(!is.na(htitle), PID %in% pids) |>
    inner_join(journals_match, by = c("htitle" = "title_int", "Year")) |>
    select(PID, Level)
  if(closest_if_missing){
    pids_na <- host_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$htitle)
    closest <- get_closest(pubs_na, journals_na, "htitle", "title_int")
    host_int <- host_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_int <- host_int |>
    filter(!is.na(Level)) |>
    mutate(matched = "HostPublication")

  bind_rows(title_title, title_int, series_title, series_int, host_title, host_int) |>
    distinct()
}

#' Match DiVA publications with Norwegian List journals by conference
#'
#' @param journals list of journal levels by year
#' @param publications publication list from DiVA
#' @param closest_if_missing set to TRUE to try closest year if level is missing
#' @import dplyr
#' @export
match_diva_conference <- function(journals, publications, closest_if_missing = FALSE){

  journals_match <- journals |>
    mutate(title = clean_conference(Title_original),
           title_int = clean_conference(Title_international),
           title_extra = extra_clean_conference(Title_original),
           title_int_extra = extra_clean_conference(Title_international))

  pubs_match <- publications |>
    filter(!is.na(coalesce(Journal, Series, HostPublication))) |>
    mutate(host = clean_conference(HostPublication),
           host_extra = extra_clean_conference(HostPublication),
           series = clean_conference(Series),
           series_extra = extra_clean_conference(Series),
           conf = clean_conference(Conference),
           conf_extra = extra_clean_conference(Conference))

  pids <- pubs_match$PID

  host_title <- pubs_match |>
    filter(!is.na(host)) |>
    inner_join(journals_match, by = c("host" = "title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- host_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$host)
    closest <- get_closest(pubs_na, journals_na, "host", "title")
    host_title  <- host_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_title  <- host_title |>
    filter(!is.na(Level))  |>
    mutate(matched = "HostPublication")

  pids <- setdiff(pids, host_title$PID)

  host_int <- pubs_match |>
    filter(!is.na(host), PID %in% pids) |>
    inner_join(journals_match, by = c("host" = "title_int", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- host_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$host)
    closest <- get_closest(pubs_na, journals_na, "host", "title_int")
    host_int  <- host_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_int <- host_int |>
    filter(!is.na(Level))|>
    mutate(matched = "HostPublication")

  pids <- setdiff(pids, host_int$PID)

  host_extra <- pubs_match |>
    filter(!is.na(host_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("host_extra" = "title_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- host_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_extra %in% pubs_na$host_extra)
    closest <- get_closest(pubs_na, journals_na, "host_extra", "title_extra")
    host_extra <- host_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_extra <- host_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "HostPublication")

  pids <- setdiff(pids, host_extra$PID)

  host_int_extra <- pubs_match |>
    filter(!is.na(host_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("host_extra" = "title_int_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- host_int_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int_extra %in% pubs_na$host_extra)
    closest <- get_closest(pubs_na, journals_na, "host_extra", "title_int_extra")
    host_int_extra  <- host_int_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  host_int_extra <- host_int_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "HostPublication")

  pids <- setdiff(pids, host_int_extra$PID)

  series_title <- pubs_match |>
    filter(!is.na(series), PID %in% pids) |>
    inner_join(journals_match, by = c("series" = "title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$series)
    closest <- get_closest(pubs_na, journals_na, "series", "title")
    series_title  <- series_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_title  <- series_title |>
    filter(!is.na(Level))  |>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_title$PID)

  series_int <- pubs_match |>
    filter(!is.na(series), PID %in% pids) |>
    inner_join(journals_match, by = c("series" = "title_int", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$series)
    closest <- get_closest(pubs_na, journals_na, "series", "title_int")
    series_int  <- series_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_int <- series_int |>
    filter(!is.na(Level))|>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_int$PID)

  series_extra <- pubs_match |>
    filter(!is.na(series_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("series_extra" = "title_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_extra %in% pubs_na$series_extra)
    closest <- get_closest(pubs_na, journals_na, "series_extra", "title_extra")
    series_extra <- series_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_extra <- series_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_extra$PID)

  series_int_extra <- pubs_match |>
    filter(!is.na(series_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("series_extra" = "title_int_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- series_int_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int_extra %in% pubs_na$series_extra)
    closest <- get_closest(pubs_na, journals_na, "series_extra", "title_int_extra")
    series_int_extra <- series_int_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  series_int_extra <- series_int_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "Series")

  pids <- setdiff(pids, series_int_extra$PID)

  conf_title <- pubs_match |>
    filter(!is.na(conf), PID %in% pids) |>
    inner_join(journals_match, by = c("conf" = "title", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- conf_title |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title %in% pubs_na$conf)
    closest <- get_closest(pubs_na, journals_na, "conf", "title")
    conf_title  <- conf_title |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  conf_title  <- conf_title |>
    filter(!is.na(Level))  |>
    mutate(matched = "Conference")

  pids <- setdiff(pids, conf_title$PID)

  conf_int <- pubs_match |>
    filter(!is.na(conf), PID %in% pids) |>
    inner_join(journals_match, by = c("conf" = "title_int", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- conf_int |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int %in% pubs_na$conf)
    closest <- get_closest(pubs_na, journals_na, "conf", "title_int")
    conf_int  <- conf_int |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  conf_int <- conf_int |>
    filter(!is.na(Level))|>
    mutate(matched = "Conference")

  pids <- setdiff(pids, conf_int$PID)

  conf_extra <- pubs_match |>
    filter(!is.na(conf_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("conf_extra" = "title_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- conf_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_extra %in% pubs_na$conf_extra)
    closest <- get_closest(pubs_na, journals_na, "conf_extra", "title_extra")
    conf_extra <- conf_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  conf_extra <- conf_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "Conference")

  pids <- setdiff(pids, conf_extra$PID)

  conf_int_extra <- pubs_match |>
    filter(!is.na(conf_extra), PID %in% pids) |>
    inner_join(journals_match, by = c("conf_extra" = "title_int_extra", "Year")) |>
    select(PID, Level)

  if(closest_if_missing){
    pids_na <- conf_int_extra |> filter(is.na(Level)) |> pull(PID)
    pubs_na <- pubs_match |> filter(PID %in% pids_na)
    journals_na <- journals_match |> filter(title_int_extra %in% pubs_na$conf_extra)
    closest <- get_closest(pubs_na, journals_na, "conf_extra", "title_int_extra")
    conf_int_extra <- conf_int_extra |>
      filter(!PID %in% closest$PID) |>
      bind_rows(closest)
  }

  conf_int_extra <- conf_int_extra |>
    filter(!is.na(Level))|>
    mutate(matched = "Conference")

  bind_rows(host_title, host_int, host_extra, host_int_extra,
            series_title, series_int, series_extra, series_int_extra,
            conf_title, conf_int, conf_extra, conf_int_extra) |>
    distinct()
}
