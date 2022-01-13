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

      # TODO: load data model config files
      # self.analyses = self._load_config("analyses")
      # self.assays = self._load_config("assays")
      # self.platforms = self._load_config("platforms")
    },

    #' @description
    #' Builds Io/DataDAG data package.
    #'
    #' @param recipe Io x DataDAG recipe
    #' @param resources list List of resources to include in package
    build_package = function(recipe, resources) {
      # load & validate recipe
      self$recipe <- private$load_recipe(recipe)

      pkg <- create_package()

      i <- 1

      for (resource_name in names(resources)) {
        pkg <- pkg %>%
          add_resource(resource_name, resources[[resource_name]])

        pkg$resources[[i]]$num_rows <- nrow(resources[[resource_name]])
        pkg$resources[[i]]$num_columns <- ncol(resources[[resource_name]])

        i <- 1 + 1;
      }

      #pkg$resources[[1]]$type <- "dataset"
      #pkg$resources[[length(pkg$resources)]]$type <- "row-metadata"
      #pkg$resources[[length(pkg$resources)]]$type <- "column-metadata"

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
      mdata$provenance$nodes[[mdata$data$uuid]][["software"]] <- list(
        "name" = "io-r",
        "time" = now,
        "version" = private$version
      )

      # add io metadata section
      pkg$io <- mdata

      return(pkg)
    },

    #' @description
    #' Builds a new Io/DataDAG data package, using an old one
    #'
    #' @param path Path to original datapackage
    #' @param metadata List with changes to be made to the metadata
    #' @param action Name of action to use in provenance entry
    #' @param resources list List of resources to include in data package
    #' @param annotations list (Optional) list of markdown annotations
    #' @param views list (Optional) list of vega-lite views to include
    #' 
    update_package = function(path, metadata, action, resources,
                              annotations=list(),
                              views=list()) {
      # extract io block from previous datapackage, and apply any updates
      if (!file.exists(path)) {
        stop(glue("[Error] No datapackage found at {path}!"))
      }

      prev_pkg <- read_package(path)

      self$recipe <- prev_pkg$io
      self$recipe <- modifyList(self$recipe, metadata)

      # create new data package and add resources
      pkg <- create_package()

      i <- 1

      for (resource_name in names(resources)) {
        pkg <- pkg %>%
          add_resource(resource_name, resources[[resource_name]])

        pkg$resources[[i]]$num_rows <- nrow(resources[[resource_name]])
        pkg$resources[[i]]$num_columns <- ncol(resources[[resource_name]])

        i <- 1 + 1;
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
        software = list(
          "name" = "io-r",
          "time" = now,
          "version" = private$version
        )
      )

      new_edge <- list(list(source = prev_pkg$io$data$uuid, target = mdata$data$uuid))

      mdata$provenance$nodes[[mdata$data$uuid]] <- new_node
      mdata$provenance$edges <- c(mdata$provenance$edges, new_edge)

      # add annotations
      mdata$annotations <- annotations

      # add views, if present
      for (view_name in names(views)) {
        view <- views[[view_name]]

        pkg$views[[view_name]] <- list(
          "name" = view_name,
          "title" = view$description,
          "resources" = view$data,
          "specType" = "vega-lite",
          "spec" = view
        )
      }

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
