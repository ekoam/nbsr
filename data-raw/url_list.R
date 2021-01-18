url_list <- c(
  "en" = "https://data.stats.gov.cn/english/easyquery.htm",
  "zh" = "https://data.stats.gov.cn/easyquery.htm"
)

usethis::use_data(url_list, overwrite = TRUE, internal = TRUE)
