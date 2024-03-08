#' Create a CloudRun job.
#'
#' Deploys an existing gcr.io image as a Job
#'
#' @seealso \href{https://cloud.google.com/run/}{Google Documentation for Cloud Run}
#' @seealso Use \link{cr_deploy_docker} or similar to create image, \link{cr_deploy_run} to automate building and deploying, \link{cr_deploy_plumber} to deploy plumber APIs.
#'
#' @param image The name of the image to create or use in deployment - \code{gcr.io}
#' @param name Name for deployment on Cloud Run
#' @param region The endpoint region for deployment
#' @param projectId The GCP project from which the jobs should be listed
#' @param memory The format for size is a fixed or floating point number followed by a unit: G, M, or K corresponding to gigabyte, megabyte, or kilobyte, respectively, or use the power-of-two equivalents: Gi, Mi, Ki corresponding to gibibyte, mebibyte or kibibyte respectively. The default is 256Mi
#' @param cpu 1 or 2 CPUs for your instance
#' @param env_vars Environment arguments passed to the Cloud Run container at runtime.  Distinct from \code{env} that run at build time.
#' @param gcloud_args a character string of arguments that can be sent to the gcloud command not covered by other parameters of this function
#' @param ... Other arguments passed to \link{cr_buildstep_run}
#' @inheritDotParams cr_buildstep_run
#'
#' @inheritParams cr_build
#' @importFrom googleAuthR gar_api_generator
#' @family Cloud Run functions
#'
#' @details
#'
#'  Uses Cloud Build to deploy an image to Cloud Run
#'
#' @seealso \href{https://cloud.google.com/build/docs/deploying-builds/deploy-cloud-run}{Deploying Cloud Run using Cloud Build}
#'
#' @export
#' @examples
#' \dontrun{
#' cr_project_set("my-project")
#' cr_region_set("europe-west1")
#' cr_run("gcr.io/my-project/my-image")
#' cr_run("gcr.io/cloud-tagging-10302018/gtm-cloud-image:stable",
#'   env_vars = c("CONTAINER_CONFIG=xxxxxxx")
#' )
#' }
cr_run_job <- function(image,
                   name = basename(image),
                   task_count = 1,
                   max_retries = 3,
                   memory = "256Mi",
                   cpu = 1,
                   timeout = 600L,
                   region = cr_region_get(),
                   projectId = cr_project_get(),
                   launch_browser = interactive(),
                   env_vars = NULL,
                   gcloud_args = NULL,
                   ...) {
  myMessage(paste("#> Launching CloudRun image: ", image),
    level = 3
  )

  assert_that(
    is.string(image),
    is.string(name)
  )

  # use cloud build to deploy
  run_yaml <- cr_build_yaml(
    steps = c(
      cr_buildstep_docker_auth(image = image),
      cr_buildstep_run_job(
        name = name,
        image = image,
        region = region,
        parallelism = parallelism,
        task_count = task_count,
        max_retries = max_retries,
        memory = memory,
        cpu = cpu,
        env_vars = env_vars,
        gcloud_args = gcloud_args,
        ...
      )
    )
  )

  build <- cr_build(run_yaml,
    projectId = projectId,
    timeout = timeout,
    launch_browser = launch_browser
  )

  result <- cr_build_wait(build, projectId = projectId)

  if (result$status == "SUCCESS") {
    run <- cr_run_job_get(name, projectId = projectId)
    myMessage(paste(
      "#> Running at: ",
      run$status$url
    ), level = 3)

    if (launch_browser) utils::browseURL(run$status$url)

    return(run)
  } else {
    myMessage("#Problem deploying to Cloud Run Job", level = 3)
    return(result)
  }
}



make_endpoint <- function(endbit) {
  region <- .cr_env$region

  if (is.null(region)) {
    region <- Sys.getenv("CR_REGION")
    .cr_env$region <- region
  }

  if (is.null(region) || region == "") {
    stop("Must select region via cr_region_set() or set environment CR_REGION",
      call. = FALSE
    )
  }

  sprintf(
    # "https://%s-run.googleapis.com/apis/serving.knative.dev/v1/%s",
    "https://%s-run.googleapis.com/apis/run.googleapis.com/v1/%s", # JZ
    region, endbit
  )
}


#' List CloudRun jobs.
#'
#' List the Cloud Run jobs you have access to
#'
#' @seealso \href{https://cloud.google.com/run/}{Google Documentation for Cloud Run}
#'
#' @param projectId The GCP project from which the jobs should be listed
#' @param labelSelector Allows to filter resources based on a label
#' @param limit The maximum number of records that should be returned
#' @param summary If TRUE will return only a subset of info available, set to FALSE for all metadata
#' @importFrom googleAuthR gar_api_generator
#' @family Cloud Run functions
#' @export
cr_run_job_list <- function(projectId = cr_project_get(),
                        labelSelector = NULL,
                        limit = NULL,
                        summary = TRUE) {
  assert_that(
    is.flag(summary)
  )

  url <- make_endpoint(sprintf("namespaces/%s/jobs", projectId)) # JZ
  myMessage("Cloud Run jobs in region: ",
    .cr_env$region,
    level = 3
  )
  # run.namespaces.jobs.list
  # TODO: paging
  pars <- list(
    labelSelector = labelSelector,
    continue = NULL,
    limit = limit
  )
  f <- gar_api_generator(url,
    "GET",
    pars_args = rmNullObs(pars),
    data_parse_function = parse_job_list,
    checkTrailingSlash = FALSE
  )
  o <- f()

  if (!summary) {
    return(o)
  }
  # print(o)
  # print(tibble::as_tibble(o))
  # print(str(o))
  # print(rjson::toJSON(o))
  parse_job_list_post(o)
}

#' @noRd
#' @import assertthat
parse_job_list <- function(x) {
  # print("x$kind")
  # print(x$kind)
  assert_that(
    x$kind == "JobList"
  )

  x$items
}

parse_job_list_post <- function(x) {
#  print(str(x$spec$template$spec$template$spec$containers))
#  print(str(x$spec$template$spec$template$spec))
#  print(str(x$spec$template$spec$template))
#  print(str(x$metadata$name))
  data.frame(
    name = x$metadata$name,
    container = unlist(lapply(
      x$spec$template$spec$template$spec$containers,
      function(x) x$image
    )),
    taskCount = x$spec$template$spec$taskCount,
    # url = x$status$url, JZ
    stringsAsFactors = FALSE
  )
}

#' Get information about a Cloud Run job.
#'
#'
#' @seealso \href{https://cloud.google.com/run/docs/reference/rest/v1/namespaces.jobs/get}{Google Documentation on namespaces.jobs.get}
#'
#' @details This returns details on a particular deployed Cloud Run job.
#'
#' @param name The name of the job to retrieve
#' @param projectId The projectId to get from
#'
#' @importFrom googleAuthR gar_api_generator
#' @family Cloud Run functions
#' @export
cr_run_job_get <- function(name, projectId = cr_project_get()) {
  url <- make_endpoint(sprintf(
    "namespaces/%s/jobs/%s",
    projectId, name
  ))

  # run.namespaces.jobs.get
  f <- gar_api_generator(url, "GET",
    data_parse_function = parse_job_get,
    checkTrailingSlash = FALSE
  )

  err_404 <- sprintf("Cloud Run: %s in project %s not found",
                     name, projectId)

  handle_errs(f, http_404 = cli::cli_alert_danger(err_404))

}

#' @import assertthat
#' @noRd
parse_job_get <- function(x) {
  assert_that(
    x$kind == "Job"
  )

  structure(
    x,
    class = c("gar_Job", "list")
  )
}
