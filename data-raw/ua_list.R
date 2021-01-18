## code to prepare `ua_list` dataset goes here
fake_user_agent <- function() {
  ua_list <- purrr::compose(
    rvest::html_text,
    ~rvest::html_nodes(., "li>a"),
    xml2::read_html
  )("http://www.useragentstring.com/pages/useragentstring.php?typ=Browser")
  assign("ua_list", ua_list, envir = .GlobalEnv)
}; fake_user_agent()

usethis::use_data(ua_list, overwrite = TRUE, internal = TRUE)
