#' An R6 class for assisting with the packaging of "Io" x "DataDAG" formatted data packages
#'
#' @importFrom frictionless add_resource create_package
#' @importFrom glue glue
#' @importFrom R6 R6Class
#' @importFrom uuid UUIDgenerate
#' @importFrom yaml read_yaml
#' @name Packager
#' @export
#'
NULL

Packager <- R6Class("Packager",
  public = list(
    #' @field recipe Input Io recipe.
    recipe = NULL,

    #' @description
    #' Creates a new "Packager" instance.
    #' @return A new `Packager` object.
    initialize = function() {
      private$conf_dir <- file.path(getwd(), "conf")
      private$schema_dir <- file.path(getwd(), "schema")

      private$config <- private$load_io_config()

      # load & validate recipe
      self$recipe <- private$load_recipe(recipe)

      # TODO: load data model config files
      # self.analyses = self._load_config("analyses")
      # self.assays = self._load_config("assays")
      # self.platforms = self._load_config("platforms")
    },

    #' @description
    #' Builds Io/DataDAG data package.
    #'
    #' @param pkg_dir Path where data package should be generated
    #' @param data data.frame Main dataset
    #' @param row_metadata data.frame (Optional) row metadata
    #' @param col_metadata data.frame (Optional) col metadata
    build_package = function(pkg_dir, data, row_metadata=NULL, col_metadata=NULL) {
      setwd(pkg_dir)

      # create package and add resources
      pkg <- create_package() %>%
        add_resource("data", expr)

      pkg$resources[[1]]$type <- "dataset"

      if (!is.null(row_metadata)) {
        pkg <- pkg %>%
          add_resource("row-metadata", row_metadata)

        pkg$resources[[length(pkg$resources)]]$type <- "row-metadata"
      }
      if (!is.null(col_metadata)) {
        pkg <- pkg %>%
          add_resource("column-metadata", col_metadata)
        
        pkg$resources[[length(pkg$resources)]]$type <- "column-metadata"
      }

      # iso8601 date string
      now <- strftime(as.POSIXlt(Sys.time(), "UTC"), "%Y-%m-%dT%H:%M:%S%z")

      # create custom "io" metadata section
      mdata <- self$recipe

      mdata$provenance <- list(
        "nodes" = list(
          "root" = mdata$provenance,
          "io-packager" = list(
            "time" = now,
            "version" = private$version
          )
        ),
        "edges" = list(
          c("root", "io-packager")
        )
      )

      mdata$contributors <- private$config$metadata$contributors
      mdata$uuid <- UUIDgenerate()

      # add io metadata section
      pkg$io <- mdata

      return(pkg)
    }
  ),
  private = list(
    config = NULL,
    conf_dir = NULL,
    schema_dir = NULL,
    version = "0.5.0",

    load_io_config = function() {
      infile <- file.path(Sys.getenv('XDG_CONFIG_HOME'), "io", "config.yml")

      if (!file.exists(infile)) {
        stop(glue("[Error] cannot find config file at {infile}!"))
      }

      return(yaml::read_yaml(infile))
    },

    load_recipe = function(path) {
      if (!file.exists(path)) {
        stop(glue("[Error] No recipe file found at {path}!"))
      }

      recipe <- yaml::read_yaml(path)

      # TODO: load & validate schema
      # schema_path <- file.path(private$schema_dir, "recipe.yml")
      return(recipe)
    }
  )
)
