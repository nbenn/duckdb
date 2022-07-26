#' @rdname duckdb_result-class
#' @inheritParams DBI::dbHasCompleted
#' @usage NULL
dbHasCompleted__duckdb_result_arrow <- function(res, ...) {
  if (!res@env$open) {
    stop("result has already been cleared")
  }

  if (is.null(res@env$resultset)) {
    FALSE
  } else if (res@stmt_lst$type == "SELECT") {
    res@env$rows_fetched == nrow(res@env$resultset)
  } else {
    TRUE
  }
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbHasCompleted", "duckdb_result_arrow", dbHasCompleted__duckdb_result_arrow)
