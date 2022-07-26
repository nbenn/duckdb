#' @rdname duckdb_result-class
#' @inheritParams DBI::dbFetch
#' @importFrom utils head
#' @usage NULL
dbFetch__duckdb_result_arrow <- function(res, n = -1, ...) {

  if (!res@env$open) {
    stop("result set was closed")
  }

  if (isTRUE(is.na(n))) {
    if (!exists("rb_reader", envir = res@env)) {
      assign("rb_reader", duckdb_fetch_record_batch(res, res@chunk_size), envir = res@env)
    }
    out <- res@env$rb_reader$read_next_batch()
    if (!exists("df_ptype", envir = res@env)) {
      assign("df_ptype", head(out, n = 0), envir = res@env)
    }
    if (is.null(out)) {
      # technically too late; how to fix?
      assign("is_complete", TRUE, envir = res@env)
      out <- res@env$df_ptype
    }
    res@env$rows_fetched <- res@env$rows_fetched + nrow(out)
    return(out)
  }

  if (n != -1) {
    stop("Cannot dbFetch() an Arrow result unless n = -1")
  }

  if (exists("rb_reader", envir = res@env)) {
    # maybe coerce to rb?
    out <- res@env$rb_reader$read_table()
    if (is.null(out)) {
      out <- res@env$df_ptype
    }
    assign("is_complete", TRUE, envir = res@env)
    return(out)
  }

  out <- duckdb_fetch_arrow(res, res@chunk_size)
  assign("is_complete", TRUE, envir = res@env)
  res@env$rows_fetched <- res@env$rows_fetched + nrow(out)

  out
}

#' @rdname duckdb_result-class
#' @export
setMethod("dbFetch", "duckdb_result_arrow", dbFetch__duckdb_result_arrow)
