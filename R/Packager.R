#' An R6 class for assisting with the packaging of "Io" x "DataDAG" formatted data packages
#'
#' @importFrom frictionless add_resource create_package read_package
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
    #'
    #' @return A new `Packager` object.
    initialize = function() {
      private$conf_dir <- file.path(getwd(), "conf")
      private$schema_dir <- file.path(getwd(), "schema")

      private$config <- private$load_io_config()

      # TODO: load data model config files
      # self.analyses = self._load_config("analyses")
      # self.assays = self._load_config("assays")
      # self.platforms = self._load_config("platforms")
    },

    #' @description
    #' Builds Io/DataDAG data package.
    #'
    #' @param recipe Io x DataDAG recipe
    #' @param data data.frame Main dataset
    #' @param row_metadata data.frame (Optional) row metadata
    #' @param col_metadata data.frame (Optional) col metadata
    build_package = function(recipe, data, row_metadata=NULL, col_metadata=NULL) {
      # load & validate recipe
      self$recipe <- private$load_recipe(recipe)

      # create package and add resources
      pkg <- create_package() %>%
        add_resource("data", data)

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

      # generate dataset uuid and add to metadata
      mdata$data$uuid <- UUIDgenerate()

      # convert recipe provenance section to a dag
      mdata$provenance <- list(
        "nodes" = setNames(list(mdata$provenance), mdata$data$uuid),
        "edges" = list()
      )

      # add packager-related details
      mdata$provenance$nodes[[mdata$data$uuid]]$io <- list(
        "name" = "io-r",
        "time" = now,
        "version" = private$version
      )

      mdata$contributors <- private$config$metadata$contributors

      # add io metadata section
      pkg$io <- mdata

      return(pkg)
    },

    #' @description
    #' Builds a new Io/DataDAG data package, using an old one
    #'
    #' @param path Path to original datapackage
    #' @param updates List with changes to be made to the metadata
    #' @param action Name of action to use in provenance entry
    #' @param data data.frame Main dataset
    #' @param row_metadata data.frame (Optional) row metadata
    #' @param col_metadata data.frame (Optional) col metadata
    update_package = function(path, updates, action, data, row_metadata=NULL, col_metadata=NULL) {
      # extract io block from previous datapackage, and apply any updates
      if (!file.exists(path)) {
        stop(glue("[Error] No datapackage found at {path}!"))
      }

      prev_pkg <- read_package(path)

      self$recipe <- prev_pkg$io
      self$recipe <- modifyList(self$recipe, updates)

      # create new data package and add resources
      pkg <- create_package() %>%
        add_resource("data", data)

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

      # generate dataset uuid and add to metadata
      mdata$data$uuid <- UUIDgenerate()

      # update provenance DAG
      new_node <- list(
        action = action,
        io = list(
          "name" = "io-r",
          "time" = now,
          "version" = private$version
        )
      )

      new_edge <- list(list(source=prev_pkg$io$data$uuid, target=mdata$data$uuid))

      mdata$provenance$nodes[[mdata$data$uuid]] <- new_node
      mdata$provenance$edges <- c(mdata$provenance$edges, new_edge)

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
