#' @rdname duckdb_result-class
#' @inheritParams DBI::dbClearResult
#' @usage NULL
dbClearResult__duckdb_result_arrow <- function(res, ...) {
  if (res@env$open) {
    rapi_release(res@stmt_lst$ref)
    res@env$open <- FALSE
  } else {
    warning("Result was cleared already")
  }
  return(invisible(TRUE))
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbClearResult", "duckdb_result_arrow", dbClearResult__duckdb_result_arrow)
