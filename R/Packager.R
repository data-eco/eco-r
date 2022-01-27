#' An R6 class for assisting with creating and interacting with "eco" data packages
#' packages
#'
#' @importFrom frictionless add_resource create_package read_package
#' @importFrom glue glue
#' @importFrom R6 R6Class
#' @importFrom jsonlite read_json
#' @importFrom uuid UUIDgenerate
#' @importFrom yaml read_yaml
#' @name Packager
#' @export
#'
NULL

Packager <- R6Class("Packager",
  public = list(
    #' @field recipe Input eco recipe.
    recipe = NULL,

    #' @description
    #' Creates a new "Packager" instance.
    #'
    #' @return A new `Packager` object.
    initialize = function() {
      private$schema_dir <- system.file("extdata", "profiles", package = "eco")
    },

    #' @description
    #' Builds an eco data package.
    #'
    #' @param resources       List of resources (dataframes/tibbles) to include in package
    #' @param annotations     Vector of filepaths to annotation files, or string annotations
    #' @param views           List of filepaths to vega-lite views, or list representations of such views
    #' @param node_metadata   [Optional] Additional node-level metadata to include,
    #'                        either as a list, or path to a yml/json file; Node level metadata is *not*
    #'                        carried over to child nodes.
    #' @param dag_metadata    [Optional] Additional dag-level metadata to include,
    #'                        either as a list, or path to a yml/json file; DAG-level metadata *is* copied over
    #'                        to child packages by default; if the same field is specified downstream, the
    #'                        value of the field is updated.
    #' @param profile         [Optional] Name of metadata profile to use for validation, or "", for none
    #' @param pkg_dir         [Optional] Location where data package should be saved (default: "./")
    #' @param include_summary [Optional] Whether or not to compute & embed summary statistics at the time of package creation (default: FALSE)
    build_package = function(resources, annotations=c(), views=c(), 
                             node_metadata=list(),
                             dag_metadata=list(), profile="", pkg_dir="./",
                             include_summary=FALSE) {
      # parse dag- and node-level metadata
      node_mdata <- private$parse_metadata(node_metadata)
      dag_mdata <- private$parse_metadata(dag_metadata)

      # if a metadata profile was specified, validate metadata against its schema
      # TODO (Jan22)
      # if (profile != "") {
      #     private$validate_metadata(node_mdata, profile)
      # }

      # determine location to build packages
      pkg_dir <- normalizePath(pkg_dir)

      # initialize data package
      pkg <- create_package()

      # add resources to data package
      i <- 1

      for (resource_name in names(resources)) {
        pkg <- pkg %>%
          add_resource(resource_name, resources[[resource_name]])

        # add num rows/columns to metadata;
        # pkg$resources[[i]]$num_rows <- nrow(resources[[resource_name]])
        # pkg$resources[[i]]$num_columns <- ncol(resources[[resource_name]])

        i <- 1 + 1;
      }

      # generate dataset uuid and add to metadata
      node_uuid <- UUIDgenerate()

      node <- private$create_node(annotations, views)

      node$metadata <- node_mdata

      # add "eco" section to data package metadata
      pkg$eco <- list(
          "uuid" = node_uuid,
          "nodes" = list(),
          "edges" = c()
      )
      pkg$eco$nodes[[node_uuid]] <- node

      # add dag-level metadata fields, if provided
      pkg$eco[["metadata"]] <- dag_mdata

      #reserved_keys <- names(pkg$eco)

      # kwargs <- list(...)
      #
      # for (key in kwargs) {
      #     if (key %in% reserved_keys) {
      #         stop(glue("Attempting to use reserved metadata field: {key}"))
      #     }
      #
      #     pkg$eco[[key]] <- kwargs[[key]]
      # }

      # write data package to disk
      pkg %>%
        write_package(pkg_dir)
    },


    #' @description
    #' Updates an existing eco datapackage.
    #'
    #' @param existing         Path to existing datapackage.json to be extended
    #' @param resources        List of names resources to include in the data package
    #' @param annotations      Vector of filepaths to annotation files, or string annotations
    #' @param views            Vector of filepaths to vega-lite views, or dict representations of such views
    #' @param node_metadata    [Optional] Additional node-level metadata to include,
    #'                         either as a list, or path to a yml/json file; Node level metadata is *not*
    #'                         carried over to child nodes.
    #' @param dag_metadata     [Optional] Additional dag-level metadata to include,
    #'                         either as a list, or path to a yml/json file; DAG-level metadata *is* copied over
    #'                         to child packages by default; if the same field is specified downstream, the
    #'                         value of the field is updated.
    #' @param profile          [Optional] Name of metadata profile to use for validation, or "", for none
    #' @param pkg_dir          [Optional] Location where data package should be saved (default: "./")
    #' @param include_summary  [Optional] Whether or not to compute & embed summary statistics at the time of package creation (default: False)
    update_package = function(existing, resources, annotations=c(), views=c(),
                              node_metadata=list(), dag_metadata=list(), 
                              profile="", pkg_dir="./",
                              include_summary=False) {
        # determine location to build packages
        pkg_dir <- normalizePath(pkg_dir)

        # parse dag- and node-level metadata
        node_mdata <- private$parse_metadata(node_metadata)
        dag_mdata <- private$parse_metadata(dag_metadata)

        # initialize data package
        pkg <- create_package()

        # add resources to data package
        i <- 1

        for (resource_name in names(resources)) {
          pkg <- pkg %>%
            add_resource(resource_name, resources[[resource_name]])
          i <- 1 + 1;
        }

        node <- private$create_node(annotations, views, action = 'update_package')

        node$metadata <- node_mdata

        # extract io metadata + dag from existing data package
        if (!file.exists(existing)) {
            stop(glue("Invalid path to existing data package provided: {existing}"))
        }

        existing_mdata <- jsonlite::read_json(existing)

        # generate dataset uuid and add to metadata
        node_uuid <- UUIDgenerate()

        # copy over metadata section from previous stage
        pkg$eco <- existing_mdata$eco

        # apply any changes to dag-level metadata
        # limitation: changes must currently be made at the top-level of metadata..
        for (key in names(dag_mdata)) {
          pkg$eco$metadata[[key]] <- dag_mdata[[key]]
        }

        # store uuid of previous step, and update
        prev_uuid <- pkg$eco$uuid
        pkg$eco$uuid <- node_uuid

        # update provenance DAG
        pkg$eco$nodes[[node_uuid]] <- node
        pkg$eco$edges <- append(pkg$eco$edges, list(
            "source" = prev_uuid,
            "target" = node_uuid
        ))

        # write data package to disk
        pkg %>%
          write_package(pkg_dir)
    }
  ),
  private = list(
    config = NULL,
    conf_dir = NULL,
    schema_dir = NULL,
    version = "0.6.0",

    # @description
    # Generates metadata block for a new node in the DAG
    #
    # @param annotations Vector of filepaths to annotation files, or string annotations
    # @param views Vector of filepaths to vega-lite views, or list representations of such views
    # @param action String to include as the "action" used to generate the node.
    create_node = function(annotations, views, action='build_package') {
        # iso8601 date string
        now <- strftime(as.POSIXlt(Sys.time(), "UTC"), "%Y-%m-%dT%H:%M:%S%z")

        node <- list(
            "name" = "io-r",
            "action" = action,
            "time" =  now,
            "version" = private$version,
            "annot" = c(),
            "views" = c()
        )

        # add annotations and views, if present
        for (annot in annotations) {
            node[["annot"]] <- append(node[["annot"]], private$parse_annotation(annot))
        }

        for (view in views) {
            node[["views"]] <- append(node[["views"]], private$parse_view(view))
        }

        node
    },

    # @description
    # Parses metadata and validates it against a specific profile, if specified
    #
    # @param mdata Path to metadata yml|json, or, a list representation
    #
    # @return metadata as a list
    parse_metadata = function(mdata)  {
      # if path specified, attempt to load mdata from file
      if (!is.list(mdata)) {
        if (!file.exists(mdata)) {
          stop("Unable to find metadata at specified location!")
        }

        # load yaml
        if (endsWith(mdata, "yml") || (endsWith(mdata, "yaml"))) {
          mdata <- yaml::read_yaml(mdata)
        } else if (endsWith(mdata, "json")) {
          mdata <- jsonlite::read_json(mdata)
        }
      }
      mdata
    },

    # Parses a single annotation
    #
    # @param annot Either an annotation string, or a path to a plain-text file
    #
    # @return Annotation string
    parse_annotation = function(annot) {
        if (file.exists(annot)) {
          readr::read_file(annot)
        } else {
          annot
        }
    },

    # Parses a single view
    #
    # @param view Either a vega-lite view loaded into a list, or a path to a
    # json file containg such a view.
    #
    # @return vega-lite view as a list
    parse_view = function(view) {
        if (assertthat::is.string(view) && file.exists(view)) {
          jsonlite::read_json(view)
        } else {
          view
        }
    }
  )
)
