### this function is a parallel version of the function "get_data" ###
pget_data <- function(indicators, period = "1949-",
                      database = c("fsnd", "hgnd", "fsjd", "hgjd", "fsyd", "hgyd"),
                      language = c("en", "zh"),
                      nap = function(n, i) if (n - i > 20L) runif(1, .5, 1),
                      workers = parallel::detectCores()) {

  ls_of_urls <- c("en" = "http://data.stats.gov.cn/english/easyquery.htm",
                  "zh" = "http://data.stats.gov.cn/easyquery.htm")

  idr_f <- function(x) set_params(params(x, database[[1L]], period), ls_of_urls[language][[1L]])

  cl <- parallel::makeCluster(getOption("cl.cores", workers))
  parallel::clusterExport(cl, ls(envir = environment(query_ls)), environment(query_ls))
  parallel::clusterEvalQ(cl, include())

  tasks <- split(unname(indicators), seq_along(indicators) %% workers)

  res <-
    json_forest_handler(unlist(parallel::clusterApply(
      cl, tasks, query_ls, idr_f, nap
    ), recursive = FALSE))

  parallel::stopCluster(cl); res
}


## older and slower version of the JSON tree handler ###
json_tree_handler <- function(json_tree) {
  if (length(json_tree[["returndata"]][["datanodes"]]) < 1L)
    return(list(data = tibble(), tddn = tibble()))

  pack <- function(df, ...) df %>% {list(...)}
  `%:%` <- function(lhs, rhs) setNames(as.list(rhs), lhs)
  link_attrs <- function(ls_obj, attr1 = NULL, attr2, flat_ls = TRUE) {
    attr1 <- substitute(attr1); attr2 <- substitute(attr2)
    flatten_ <- if (flat_ls) flatten else (function(x) x)
    flatten_(map(ls_obj, ~ with(., eval(attr1) %:% eval(attr2))))
  }

  mta <-
    pluck(json_tree,
      "returndata",
      "wdnodes"
    ) %>%
    pack(
      info = link_attrs(., wdcode, wdname),
      domain = link_attrs(., wdcode, list(nodes %>% link_attrs(code, cname)))
    )

  tdd <-
    pluck(json_tree,
      "returndata",
      "wdnodes",
      function(x) detect(x, ~ .$wdcode == "zb"),
      "nodes"
    ) %>%
    link_attrs(
      list("indictor_code", "indicator_name", "unit", "description", "remark"),
      list(code, cname, unit, exp, memo),
      FALSE
    ) %>%
    bind_rows() %>%
    mutate(source = attr(json_tree, "src", TRUE))

  dta <-
    pluck(json_tree,
      "returndata",
      "datanodes"
    ) %>%
    pack(
      categ = link_attrs(., attr2 = list(wds %>% link_attrs(wdcode, valuecode))) %>% bind_rows(),
      numer = link_attrs(., attr2 = with(data, if (hasdata) data else NA_real_)) %>% unlist()
    ) %>%
    with({
      categ %>%
        imap_dfc(~ mta$info[[.y]] %:% compose(list, unname, unlist)(mta$domain[[.y]][.x])) %>%
        list(encode_ = categ, decode_ = .) %>%
        imap(~ rename_all(.x, function(i) paste0(.y, i))) %>%
        bind_cols("value" %:% list(numer))
    })

  list(data = dta, tddn = tdd)
}
