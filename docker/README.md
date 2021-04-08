## Using This Image

You will need **a PostgreSQL instance url** in the format `postgres://[username]:[password]@[host]:[port]/[database_name]`. With this URL, you can created containers for both the web application (`circ-webapp`) and for the background cron jobs that import and update books and otherwise keep the app running smoothly (`circ-scripts`). Either container can be used to initialize or migrate the database. During the first deployment against a brand new database, the first container run can use the default `SIMPLIFIED_DB_TASK='auto'` or be run manually with `SIMPLIFIED_DB_TASK='init'`. See the "Environment Variables" section below for mroe information.

### circ-webapp

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name webapp \
    -d -p 80:80 \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    lyrasis/circ-webapp:develop
```

Navigate to `http://localhost/admin` to in your browser to input or update configuration information. If you have not yet created an admin authorization protocol before, you'll need to do that before you can set other configuration.

### circ-scripts

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name scripts -d \
    -e TZ='YOUR_TIMEZONE_STRING' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    lyrasis/circ-scripts:develop
```

Using `docker exec -it scripts /bin/bash` in your console, navigate to `/var/log/simplified` in the container. After 5-20 minutes, you'll begin to see log files populate that directory.

### circ-exec

This image builds containers that will run a single script and stop. It's useful in conjunction with a tool like Amazon ECS Scheduled Tasks, where you can run script containers on a cron-style schedule.

Unlike the `circ-scripts` image, which runs constantly and executes every possible maintenance script--whether or not your configuration requires it--`circ-exec` offers more nuanced control of your Library Simplified Circulation Manager jobs. The most accurate place to look for recommended jobs and their recommended frequencies is [the existing `circ-scripts` crontab](https://github.com/NYPL-Simplified/circulation/blob/main/docker/services/simplified_crontab).

Because containers based on `circ-exec` are built, run their job, and are destroyed, it's important to configure an external log aggregator.

```
# See the section "Environment Variables" below for more information
# about the values listed here and their alternatives.
$ docker run --name refresh-materialized-views -it \
    -e SIMPLIFIED_SCRIPT_NAME='refresh_materialized_views' \
    -e SIMPLIFIED_PRODUCTION_DATABASE='postgres://[username]:[password]@[host]:[port]/[database_name]' \
    lyrasis/circ-exec:develop
```

## Environment Variables

Environment variables can be set with the `-e VARIABLE_KEY='variable_value'` option on the `docker run` command. `SIMPLIFIED_PRODUCTION_DATABASE` is the only required environment variable.

### `SIMPLIFIED_DB_TASK`

*Optional.* Performs a task against the database at container runtime. Options are:
  - `auto` : Either initializes or migrates the database, depending on if it is new or not. This is the default value.
  - `ignore` : Does nothing.
  - `init` : Initializes the app against a brand new database. If you are running a circulation manager for the first time ever, use this value to set up an Elasticsearch alias and account for the database schema for future migrations.
  - `migrate` : Migrates an existing database against a new release. Use this value when switching from one stable version to another.

### `SIMPLIFIED_PRODUCTION_DATABASE`

*Required.* The URL of the production PostgreSQL database for the application.

### `SIMPLIFIED_TEST_DATABASE`

*Optional.* The URL of a PostgreSQL database for tests. This optional variable allows unit tests to be run in the container.

### `TZ`

*Optional. Applies to `circ-scripts` only.* The time zone that cron should use to run scheduled scripts--usually the time zone of the library or libraries on the circulation manager instance. This value should be selected according to [Debian-system time zone options](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). This value allows scripts to be run at ideal times.

## Building new images

If you plan to work with stable versions of the Circulation Manager, we strongly recommend using the latest stable versions of circ-webapp and circ-scripts [published to Docker Hub](https://hub.docker.com/r/lyrasis/). However, there may come a time in development when you want to build Docker containers for a particular version of the Circulation Manager. If so, please use the instructions below.

We recommend you install at least version 18.06 of the Docker engine and version 1.24 of Docker Compose.

### > `.webapp` and `.scripts`

Determine which container you would like to build and update the tag and Dockerfile listed below accordingly.

```sh
$ docker build --build-arg version=YOUR_DESIRED_BRANCH_OR_COMMIT \
    --tag circ-scripts:development \
    --file Dockerfile.scripts \
    --no-cache .
```

You must run this command with the `--no-cache` option or the code in the container will not be updated from the last build, defeating the purpose of the build and enhancing overall confusion. Feel free to change the image tag as you like.