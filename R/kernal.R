#' @importFrom urltools param_set
#' @importFrom rjson fromJSON
#' @importFrom httr GET user_agent timeout set_config config
#' @importFrom purrr transpose
#' @importFrom tibble as_tibble
#' @importFrom dplyr if_else bind_rows bind_cols select
#' @importFrom stats runif setNames
#' @importFrom utils setTxtProgressBar txtProgressBar
# param_set <- urltools::param_set
# fromJSON <- rjson::fromJSON
# GET <- httr::GET
# user_agent <- httr::user_agent
# timeout <- httr::timeout
# set_config <- httr::set_config
# config <- httr::config
# transpose <- purrr::transpose
# as_tibble <- tibble::as_tibble
# if_else <- dplyr::if_else
# bind_rows <- dplyr::bind_rows
# bind_cols <- dplyr::bind_cols
# select <- dplyr::select

### this function generates a query string ###
set_params <- function(params, for_url) {
  Reduce(function(u, k) param_set(u, k, params[[k]]), names(params), for_url)
}


### this function generates a unix timestamp ###
gen_k1 <- function() format(as.numeric(Sys.time()) * 1000, digits = 13L)


### this function generates a list of query parameters ###
params <- function(indicator, database, over_time = NULL, def = c("idr", "prev", "toc")) {
  switch(def[[1L]],
         idr = list(m = 'QueryData',
                    dbcode = database,
                    rowcode = 'reg',
                    colcode = 'sj',
                    wds = paste0('[{"wdcode":"zb","valuecode":"', indicator, '"}]'),
                    dfwds = paste0('[{"wdcode":"sj","valuecode":"', over_time, '"}]'),
                    k1 = gen_k1()),
         prev = list(m = 'QueryData',
                     dbcode = database,
                     rowcode = 'zb',
                     colcode = 'sj',
                     wds = '[]',
                     dfwds = paste0('[{"wdcode":"zb","valuecode":"', indicator, '"}]'),
                     k1 = gen_k1()),
         toc = list(m = 'getTree',
                    dbcode = database,
                    wdcode = 'zb',
                    id = indicator))
}


recur_pluck <- function(forest, ...) {
  mas_env <- parent.frame(1)
  is.placeholder <- function(x) {
    identical(x, quote(.)) || identical(x, quote((.)))
  }
  is.pure.selector <- function(x) {
    is.character(x) || is.numeric(x)
  }
  is.selector <- function(x) {
    is.pure.selector(x) || is.function(x)
  }
  idx_eval <- function(als) {
    lapply(als, function(x) if (is.placeholder(x)) x else eval(x, mas_env, mas_env))
  }
  unlist_ <- function(x, i) {
    if (identical(i, quote((.)))) x else unlist(x, FALSE, FALSE)
  }
  select_ <- function(x, i) {
    if (is.pure.selector(i)) return(i)
    eval(i)(x)
  }
  recur_ <- function(trs, als) {
    if (length(als) < 2L) {
      if (is.placeholder(als[[1L]])) {
        unlist_(trs, als[[1L]])
      } else {
        trs[[select_(trs, als[[1L]])]]
      }
    } else {
      if (is.placeholder(als[[1L]]) && is.pure.selector(als[[2L]])) {
        recur_(transpose(trs)[[als[[2L]]]], als[-2L])
      } else if (is.selector(als[[1L]])) {
        recur_(trs[[select_(trs, als[[1L]])]], als[-1L])
      } else {
        unlist_(lapply(trs, recur_, als[-1L]), als[[1L]])
      }
    }
  }
  recur_(forest, idx_eval(eval(substitute(alist(...)))))
}


q_validate <- function(json_trees) {
  sel <- vapply(json_trees, function(x) {
    code <- gsub(".*\\{\"wdcode\":\"zb\",\"valuecode\":\"([0-9A-Z]+)\"\\}.*", "\\1", attr(x, "src", TRUE))
    has_data <- length(x[["returndata"]][["datanodes"]]) > 0
    if (!has_data) {
      warning("Invalid indicator code '", code, "'. Querier did not find such an indicator in the database.")
    }
    has_data
  }, logical(1L))
  if (all(!sel)) {
    stop("No data found.")
  }
  json_trees[sel]
}


### this function handles the JSON data received and converts it into tibbles ###
json_forest_handler <- function(json_trees) {
  src_sf <- function(f) function(...) {
    srcs <- lapply(..1, attr, "src", TRUE)
    res <- f(...)
    lapply(seq_along(res), function(x) lapply(res[[x]], `[[<-`, "src", srcs[[x]]))
  }
  funlist <- function(x) {
    unlist(x, FALSE, FALSE)
  }
  index <- function(key, val) {
    tmp <- `names<-`(val, key)
    tmp[!duplicated(tmp)]
  }
  set_i_names <- function(lval, lkey) {
    lapply(seq_along(lval), function(i) `names<-`(lval[[i]], lkey[[i]]))
  }
  collapse_names <- function(l) {
    nms <- names(l)
    unms <- unique(nms)
    res <- lapply(unms, function(x) do.call(c, unname(l[which(nms == x)])))
    `names<-`(res, unms)
  }
  rename <- function(l, f) {
    `names<-`(l, f(names(l)))
  }

  json_trees <- q_validate(json_trees)
  wdcode <- recur_pluck(json_trees, ., "returndata", "wdnodes", ., "wdcode")
  wdname <- recur_pluck(json_trees, ., "returndata", "wdnodes", ., "wdname")
  cname <- recur_pluck(json_trees, ., "returndata", "wdnodes", (.), "nodes", ., "cname")
  ccode <- recur_pluck(json_trees, ., "returndata", "wdnodes", (.), "nodes", ., "code")
  dim_info <- index(wdcode, wdname)
  dim_domain <- collapse_names(index(wdcode, set_i_names(cname, ccode)))
  rm(wdcode, wdname, cname, ccode)

  tdd <- as_tibble(lapply(transpose(funlist(src_sf(recur_pluck)(
    json_trees, (.), "returndata", "wdnodes",
    function(l) which(vapply(l, function(x) x[["wdcode"]] == "zb", logical(1L)))[[1L]],
    "nodes"
  ))), funlist))

  dwds <- recur_pluck(json_trees, ., "returndata", "datanodes", (.), "wds")
  ddata <- recur_pluck(json_trees, ., "returndata", "datanodes", ., "data", "data")
  dhasdata <- recur_pluck(json_trees, ., "returndata", "datanodes", ., "data", "hasdata")
  dwdcode <- lapply(seq_along(dwds[[1L]]), function(x) unique(recur_pluck(dwds, ., x, "wdcode")))
  dvaluecode <- lapply(seq_along(dwds[[1L]]), function(x) recur_pluck(dwds, ., x, "valuecode"))
  rm(dwds)
  numer <- if_else(dhasdata, ddata, NA_real_)
  rm(ddata, dhasdata)
  categ <- as_tibble(`names<-`(dvaluecode, dwdcode))
  categ_ <- `names<-`(lapply(names(categ), function(x) unname(dim_domain[[x]][categ[[x]]])), dim_info[names(categ)])
  rm(dwdcode, dvaluecode)
  categ_ <- rename(categ_, function(x) paste0("decode_", x))
  categ <- rename(categ, function(x) paste0("encode_", x))

  list(
    data = bind_cols(categ, categ_, list(value = numer)),
    meta = select(
      tdd, indicator_code = code, indicator_name = cname,
      unit, description = exp, remark = memo, source = src
    )
  )
}


### this function force the querier to wait for a given amount of time until it starts the next query ###
wait <- function(seconds) {
  message("- Scheduled to retry in ", seconds, "s")
  pb <- txtProgressBar(min = 0, max = 100, style = 3)
  for(i in 1:100) {
    Sys.sleep(seconds / 100)
    setTxtProgressBar(pb, i)
  }
  close(pb)
}


### this function is the main function to generate a query call to NBS.
### Currenly, it cannot bypass the antibot mechanism of the website and has to
### wait for the mechanism to deactivate before subsequent requests ###
query <- function(q_str, nap) {
  repeat {
    if (!is.null(nap)) Sys.sleep(nap)
    message("- Querying: ", basename(q_str), "...")
    delayedAssign("dat", rawToChar(GET(
      q_str, user_agent(sample(ua_list, 1L)),
      timeout(60), set_config(config(ssl_verifypeer = FALSE))
    )[["content"]]))
    tryCatch(return(
      `attr<-`(fromJSON(dat), "src", q_str)
    ), error = function(e) {
      message("- Failed at ", basename(q_str), "...\n  ", e[["message"]])
    })
    wait(as.integer(runif(1, 240, 360)))
  }
}
query_ls <- function(raw_strs, f_converter, f_nap) {
  n <- length(raw_strs)
  lapply(seq_len(n), function(i) query(
    f_converter(raw_strs[[i]]),
    if (!is.null(formals(f_nap))) f_nap(n, i) else f_nap()
  ))
}

#' Get metadata from the NBS website.
#' @description Retrieve the table of contents or the preview of all indicators
#'   from a database.
#' @param what either "toc" (i.e. table of contents) or "prev" (i.e. preview).
#' @param database the NBS database to be visited by the querier.
#'   \itemize{
#'    \item{\code{fs }}{for provincial databases.}
#'    \item{\code{hg }}{for national databases.}
#'    \item{\code{nd }}{for annual databases.}
#'    \item{\code{jd }}{for quarterly databases.}
#'    \item{\code{yd }}{for monthly databases.}
#'   }
#' @param language either "en" (i.e. English) or "zh" (i.e. Chinese).
#' @param nap let the function take a nap in-between two query calls. The nap
#'   length defaults to be a random number between 0.5s and 1s. This makes the
#'   querier less likely to be flagged by the anti-bot mechanism on the NBS
#'   website.
#' @return A dataframe that contains the metadata.
#' @examples
#' \dontrun{
#'  get_meta()
#'  get_meta("toc", "fsnd")
#' }
#' @export
get_meta <- function(what = c("toc", "prev"),
                     database = c("fsnd", "hgnd", "fsjd", "hgjd", "fsyd", "hgyd"),
                     language = c("en", "zh"),
                     nap = function() runif(1, .5, 1)) {

  q_str_f <- function(id, def) {
     set_params(params(id, database[[1L]], def = def), url_list[language][[1L]])
  }

  recur_query <- function(x = list(isParent = TRUE, id = "zb", src = ""), f = q_str_f) {
    if (x[["isParent"]]) {
      json_tree <- query(f(x[["id"]], "toc"), nap())
      c(list(x), unlist(lapply(
        json_tree,
        function(i) {i[["src"]] <- attr(json_tree, "src", TRUE); recur_query(i)}
      ), recursive = FALSE))
    } else {
      list(x)
    }
  }

  toc <- recur_query()
  if (what[[1L]] == "toc")
    return(bind_rows(toc[-1L]))

  toc <- vapply(toc, `[`, vector("list", 2L), c("isParent", "id"))
  json_forest_handler(query_ls(
    unlist(toc["id", ])[!unlist(toc["isParent", ])],
    function(x) q_str_f(x, "prev"), nap
  ))[["meta"]]
}

#' Get indicator data from the NBS website.
#' @description Retrieve indicators sequentially from a database.
#' @param indicators a character vector of indicator codes like \code{"A010101"}
#'   or \code{c("A010101", "A010102")}.
#' @param database see the \code{database} argument in \code{\link{get_meta}}.
#' @param period the period of the time series. Below shows a list of valid
#'   period filters:
#'   \itemize{
#'    \item{month: }{201201, 201205.}
#'    \item{quarter: }{2012A, 2012B, 2012C, 2012D.}
#'    \item{year: }{2012, 2013.}
#'    \item{duration: }{2013-, 2008-2009, last10.}
#'   }
#' @param language either "en" (i.e. English) or "zh" (i.e. Chinese).
#' @param nap let the function take a nap in-between two query calls. No nap to
#'   be taken for a short list of indicators (less than 20); a nap of length
#'   defaulting to be a random number between 0.5s and 1s is invoked for each
#'   indicator in addition to the first 20. This makes the querier less likely
#'   to be flagged by the anti-bot mechanism on the NBS website.
#' @return A list of two dataframes: indicators and their metadata.
#' @examples
#' \dontrun{
#'  get_data("A010101")
#'  get_data("A010101", database = "fsnd")
#' }
#' @export
get_data <- function(indicators, period = "1949-",
                     database = c("fsnd", "hgnd", "fsjd", "hgjd", "fsyd", "hgyd"),
                     language = c("en", "zh"),
                     nap = function(n, i) if (n - i > 20L) runif(1, .5, 1)) {
  idr_f <- function(x) set_params(params(x, database[[1L]], period), url_list[language][[1L]])

  json_forest_handler(query_ls(
    unname(indicators), idr_f, nap
  ))
}
