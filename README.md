# Palace Manager

[![Test](https://github.com/ThePalaceProject/circulation/actions/workflows/test.yml/badge.svg)](https://github.com/ThePalaceProject/circulation/actions/workflows/test.yml)
[![Build](https://github.com/ThePalaceProject/circulation/actions/workflows/build.yml/badge.svg)](https://github.com/ThePalaceProject/circulation/actions/workflows/build.yml)
[![codecov](https://codecov.io/github/thepalaceproject/circulation/branch/main/graph/badge.svg?token=T09QW6DLH6)](https://codecov.io/github/thepalaceproject/circulation)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
![Python: 3.11,3.12](https://img.shields.io/badge/Python-3.11%20|%203.12-blue)

This is a [The Palace Project](https://thepalaceproject.org) maintained fork of the NYPL
[Library Simplified](http://www.librarysimplified.org/) Circulation Manager.

## Installation

Docker images created from this code are available at:

- [circ-webapp](https://github.com/ThePalaceProject/circulation/pkgs/container/circ-webapp)
- [circ-scripts](https://github.com/ThePalaceProject/circulation/pkgs/container/circ-scripts)
- [circ-exec](https://github.com/ThePalaceProject/circulation/pkgs/container/circ-exec)

Docker images are the preferred way to deploy this code in a production environment.

## Git Branch Workflow

| Branch   | Python Version |
| -------- | -------------- |
| main     | Python 3       |
| python2  | Python 2       |

The default branch is `main` and that's the working branch that should be used when branching off for bug fixes or new
features.

Python 2 stopped being supported after January 1st, 2020 but there is still a `python2` branch which can be used. As of
August 2021, development will be done in the `main` branch and the `python2` branch will not be updated unless
absolutely necessary.

## Set Up

### Docker Compose

In order to help quickly set up a development environment, we include a [docker-compose.yml](./docker-compose.yml)
file. This docker-compose file, will build the webapp and scripts containers from your local repository, and start
those containers as well as all the necessary service containers.

You can give this a try by running the following command:

```shell
docker-compose up --build
```

### Python Set Up

#### Homebrew (OSX)

If you do not have Python 3 installed, you can use [Homebrew](https://brew.sh/) to install it by running the command
`brew install python3`.

If you do not yet have Homebrew, you can install it by running the following:

```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

#### Linux

Most distributions will offer Python packages. On Arch Linux, the following command is sufficient:

```sh
pacman -S python
```

#### pyenv

[pyenv](https://github.com/pyenv/pyenv) pyenv lets you easily switch between multiple versions of Python. It can be
[installed](https://github.com/pyenv/pyenv-installer) using the command `curl https://pyenv.run | bash`. You can then
install the version of Python you want to work with.

It is recommended that [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) be used to allow `pyenv`
to manage _virtual environments_ in a manner that can be used by the [poetry](#poetry) tool. The `pyenv-virtualenv`
plugin can be installed by cloning the relevant repository into the `plugins` subdirectory of your `$PYENV_ROOT`:

```sh
mkdir -p $PYENV_ROOT/plugins
cd $PYENV_ROOT/plugins
git clone https://github.com/pyenv/pyenv-virtualenv
```

After cloning the repository, `pyenv` now has a new `virtualenv` command:

```sh
$ pyenv virtualenv
pyenv-virtualenv: no virtualenv name given.
```

#### Poetry

You will need to set up a local virtual environment to install packages and run the project. This project uses
[poetry](https://python-poetry.org/) for dependency management.

Poetry can be installed using the command `curl -sSL https://install.python-poetry.org | python3 -`.

More information about installation options can be found in the
[poetry documentation](https://python-poetry.org/docs/master/#installation).

### Opensearch

Palace now supports Opensearch: please use it instead of Elasticsearch.
Elasticsearch is no longer supported.

#### Docker

We recommend that you run Opensearch with docker using the following docker commands:

```sh
docker run --name opensearch -d --rm -p 9200:9200 -e "discovery.type=single-node" -e "plugins.security.disabled=true" "opensearchproject/opensearch:1"
docker exec opensearch opensearch-plugin -s install analysis-icu
docker restart opensearch
```

### Database

#### Docker

```sh
docker run -d --name pg -e POSTGRES_USER=palace -e POSTGRES_PASSWORD=test -e POSTGRES_DB=circ -p 5432:5432 postgres:16
```

You can run `psql` in the container using the command

```sh
docker exec -it pg psql -U palace circ
```

#### Local

1. Download and install [Postgres](https://www.postgresql.org/download/) if you don't have it already.
2. Use the command `psql` to access the Postgresql client.
3. Within the session, run the following commands:

```sh
CREATE DATABASE circ;
CREATE USER palace with password 'test';
grant all privileges on database circ to palace;
```

### Redis

Redis is used as the broker for Celery and the caching layer. You can run Redis with docker using the following command:

```sh
docker run -d --name redis -p 6379:6379 redis/redis-stack-server
```

### Environment variables

#### Database

To let the application know which database to use, set the `SIMPLIFIED_PRODUCTION_DATABASE` environment variable.

```sh
export SIMPLIFIED_PRODUCTION_DATABASE="postgresql://palace:test@localhost:5432/circ"
```

#### Opensearch

To let the application know which Opensearch instance to use, you can set the following environment variables:

- `PALACE_SEARCH_URL`: The url of the Opensearch instance (**required**).
- `PALACE_SEARCH_INDEX_PREFIX`: The prefix to use for the Opensearch indices. The default is `circulation-works`.
    This is useful if you want to use the same Opensearch instance for multiple CM (optional).
- `PALACE_SEARCH_TIMEOUT`: The timeout in seconds to use when connecting to the Opensearch instance. The default is `20`
  (optional).
- `PALACE_SEARCH_MAXSIZE`: The maximum size of the connection pool to use when connecting to the Opensearch instance.
  (optional).

```sh
export PALACE_SEARCH_URL="http://localhost:9200"
```

#### Celery

We use [Celery](https://docs.celeryproject.org/en/stable/) to run background tasks. To configure Celery, you need to
pass a broker URL and a result backend url to the application.

- `PALACE_CELERY_BROKER_URL`: The URL of the broker to use for Celery. (**required**).
    - for example:
        ```sh
          export PALACE_CELERY_BROKER_URL="redis://localhost:6379/0"`

        ```
- `PALACE_CELERY_RESULT_BACKEND`: The url of the result backend to use for Celery. (**required**).
    - for example:
        ```sh
          export PALACE_CELERY_RESULT_BACKEND="redis://localhost:6379/2"`

        ```

We support overriding a number of other Celery settings via environment variables, but in most cases
the defaults should be sufficient. The full list of settings can be found in
[`service/celery/configuration.py`](src/palace/manager/service/celery/configuration.py).

#### Redis

We use Redis as the caching layer for the application. Although you can use the same redis database for both
Celery and caching, we recommend that you use a separate database for each purpose to avoid conflicts.

- `PALACE_REDIS_URL`: The URL of the Redis instance to use for caching. (**required**).
    - for example:
        ```sh
          export PALACE_REDIS_URL="redis://localhost:6379/1"
        ```
- `PALACE_REDIS_KEY_PREFIX`: The prefix to use for keys stored in the Redis instance. The default is `palace`.
    This is useful if you want to use the same Redis database for multiple CM (optional).

#### General

- `PALACE_BASE_URL`: The base URL of the application. Used to create absolute links. (optional)
- `PALACE_PATRON_WEB_HOSTNAMES`: Only web applications from these hosts can access this circulation manager. This can
   be a single hostname (`http://catalog.library.org`) or a pipe-separated list of hostnames
   (`http://catalog.library.org|https://beta.library.org`). You can also set this to `*` to allow access from any host,
   but you must not do this in a production environment -- only during development. (optional)

#### Storage Service

The application optionally uses a s3 compatible storage service to store files. To configure the application to use
a storage service, you can set the following environment variables:

- `PALACE_STORAGE_PUBLIC_ACCESS_BUCKET`: Required if you want to use the storage service to serve files directly to
  users. This is the name of the bucket that will be used to serve files. This bucket should be configured to allow
  public access to the files.
- `PALACE_STORAGE_ANALYTICS_BUCKET`: Required if you want to use the storage service to store analytics data.
- `PALACE_STORAGE_ACCESS_KEY`: The access key (optional).
    - If this key is set it will be passed to boto3 when connecting to the storage service.
    - If it is not set boto3 will attempt to find credentials as outlined in their
      [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).
- `PALACE_STORAGE_SECRET_KEY`: The secret key (optional).
- `PALACE_STORAGE_REGION`: The AWS region of the storage service (optional).
- `PALACE_STORAGE_ENDPOINT_URL`: The endpoint of the storage service (optional). This is used if you are using a
  s3 compatible storage service like [minio](https://min.io/).
- `PALACE_STORAGE_URL_TEMPLATE`: The url template to use when generating urls for files stored in the storage service
  (optional).
    - The default value is `https://{bucket}.s3.{region}.amazonaws.com/{key}`.
    - The following variables can be used in the template:
        - `{bucket}`: The name of the bucket.
        - `{key}`: The key of the file.
        - `{region}`: The region of the storage service.

#### Reporting

- `PALACE_REPORTING_NAME`: (Optional) A name used to identify the CM instance associated with generated reports.
- `PALACE_GOOGLE_DRIVE_SERVICE_INFO_JSON`: (Optional) A JSON string containing a Google Drive service account configuration.
  - c.f. [Creating service account credentials](https://developers.google.com/workspace/guides/create-credentials#service-account)
- `PALACE_GOOGLE_DRIVE_PARENT_FOLDER_ID`: (Optional) The ID for a Google Drive Parent Folder/Shared Drive.
  - e.g. Given the google drive folder at : `https://drive.google.com/drive/u/1/folders/0AGtlKYStJaC3Uk9PVZ`,
  `0AGtlKYStJaC3Uk9PVZ` (not a real folder ID) is the value that should be assigned environment variable.

#### Logging

The application uses the [Python logging](https://docs.python.org/3/library/logging.html) module for logging. Optionally
logs can be configured to be sent to AWS CloudWatch logs. The following environment variables can be used to configure
the logging:

- `PALACE_LOG_LEVEL`: The log level to use for the application. The default is `INFO`.
- `PALACE_LOG_VERBOSE_LEVEL`: The log level to use for particularly verbose loggers. Keeping these loggers at a
  higher log level by default makes it easier to troubleshoot issues. The default is `WARNING`.
- `PALACE_LOG_CLOUDWATCH_ENABLED`: Enable / disable sending logs to CloudWatch. The default is `false`.
- `PALACE_LOG_CLOUDWATCH_REGION`: The AWS region of the CloudWatch logs. This must be set if using CloudWatch logs.
- `PALACE_LOG_CLOUDWATCH_GROUP`: The name of the CloudWatch log group to send logs to. Default is `palace`.
- `PALACE_LOG_CLOUDWATCH_STREAM`: The name of the CloudWatch log stream to send logs to. Default is
  `{machine_name}/{program_name}/{logger_name}/{process_id}`. See
  [watchtower docs](https://github.com/kislyuk/watchtower#log-stream-naming) for details.
- `PALACE_LOG_CLOUDWATCH_INTERVAL`: The interval in seconds to send logs to CloudWatch. Default is `60`.
- `PALACE_LOG_CLOUDWATCH_CREATE_GROUP`: Whether to create the log group if it does not exist. Default is `true`.
- `PALACE_LOG_CLOUDWATCH_ACCESS_KEY`: The access key to use when sending logs to CloudWatch. This is optional.
    - If this key is set it will be passed to boto3 when connecting to CloudWatch.
    - If it is not set boto3 will attempt to find credentials as outlined in their
      [documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials).
- `PALACE_LOG_CLOUDWATCH_SECRET_KEY`: The secret key to use when sending logs to CloudWatch. This is optional.

#### Firebase Cloud Messaging

For Firebase Cloud Messaging (FCM) support (e.g., for notifications), `one` (and only one) of the following should be set:
- `PALACE_FCM_CREDENTIALS_JSON` - the JSON-format Google Cloud Platform (GCP) service account key  or
- `PALACE_FCM_CREDENTIALS_FILE` - the name of the file containing that key.

```sh
export PALACE_FCM_CREDENTIALS_JSON='{"type":"service_account","project_id":"<id>", "private_key_id":"f8...d1", ...}'
```

...or...

```sh
export PALACE_FCM_CREDENTIALS_FILE="/opt/credentials/fcm_credentials.json"
```

The FCM credentials can be downloaded once a Google Service account has been created.
More details in the [FCM documentation](https://firebase.google.com/docs/admin/setup#set-up-project-and-service-account)

#### Quicksight Dashboards

For generating quicksight dashboard links the following environment variable is required
`PALACE_QUICKSIGHT_AUTHORIZED_ARNS` - A dictionary of the format `"<dashboard name>": ["arn:aws:quicksight:...",...]`
where each quicksight dashboard gets treated with an arbitrary "name", and a list of "authorized arns".
The first the "authorized arns" is always considered as the `InitialDashboardID` when creating an embed URL
for the respective "dashboard name".

#### Support Contact URL

Setting this value causes a help link to be displayed on the admin web client page footer and with the sign in dialog.

```sh
export PALACE_ADMINUI_SUPPORT_CONTACT_URL=mailto:helpdesk@example.com
```

#### Analytics

Local analytics are enabled by default. S3 analytics can be enabled via the following environment variable:

- PALACE_S3_ANALYTICS_ENABLED: A boolean value to disable or enable s3 analytics. The default is false.

#### Email

To use the features that require sending emails, for example to reset the password for logged-out users, you will need
to have a working SMTP server and set some environment variables:

- `PALACE_MAIL_SERVER` - The SMTP server to use. Required if you want to send emails.
- `PALACE_MAIL_PORT` - The port of the SMTP server. Default: 25. (optional)
- `PALACE_MAIL_USERNAME` - The username to use when connecting to the SMTP server. (optional)
- `PALACE_MAIL_PASSWORD` - The password to use when connecting to the SMTP server. (optional)
- `PALACE_MAIL_SENDER` - The email address to use as the sender of the emails. (optional)

## Running the Application

As mentioned in the [pyenv](#pyenv) section, the `poetry` tool should be executed under a virtual environment
in order to guarantee that it will use the Python version you expect. To use a particular Python version,
you should create a local virtual environment in the cloned `circulation` repository directory. Assuming that
you want to use, for example, Python 3.11.1:

```sh
pyenv virtualenv 3.11.1 circ
```

This will create a new local virtual environment called `circ` that uses Python 3.11.1. Switch to that environment:

```sh
pyenv local circ
```

On most systems, using `pyenv` will adjust your shell prompt to indicate which virtual environment you
are now in. For example, the version of Python installed in your operating system might be `3.10.0`, but
using a virtual environment can substitute, for example, `3.11.1`:

```sh
$ python --version
Python 3.10.0

$ pyenv local circ
(circ) $ python --version
Python 3.11.1
```

For brevity, these instructions assume that all shell commands will be executed within a virtual environment.

Install the dependencies (including dev and CI):

```sh
poetry install
```

Install only the production dependencies:

```sh
poetry install --only main,pg
```

Run the application with:

```sh
poetry run python app.py
```

Check that there is now a web server listening on port `6500`:

```sh
curl http://localhost:6500/
```

You can start a celery worker with:

```sh
poetry run celery -A "palace.manager.celery.app" worker --concurrency 1 --pool solo --beat
```

### The Admin Interface

#### Access

By default, the application is configured to provide a built-in version of the [admin web interface](https://github.com/ThePalaceProject/circulation-admin).
The admin interface can be accessed by visiting the `/admin` endpoint:

```sh
# On Linux
xdg-open http://localhost:6500/admin/

# On MacOS
open http://localhost:6500/admin/
```

If no existing users are configured (which will be the case if this is a fresh instance of the application), the
admin interface will prompt you to specify an email address and password that will be used for subsequent logins.
Extra users can be configured later.

#### Creating A Library

Navigate to `System Configuration → Libraries` and click _Create new library_. You will be prompted to enter various
details such as the name of the library, a URL, and more. For example, the configuration for a hypothetical
library, _Hazelnut Peak_, might look like this:

![.github/readme/library.png](.github/readme/library.png)

Note that the _Patron support email address_ will appear in OPDS feeds served by the application, so make sure
that it is an email address you are happy to make public.

At this point, the _library_ exists but does not contain any _collections_ and therefore won't be of much use to anyone.

#### Adding Collections

Navigate to `System Configuration → Collections` and click _Create new collection_. You will prompted to enter
details that will be used to source the data for the collection. A good starting point, for testing purposes,
is to use an open access OPDS feed as a data source. The
[Open Bookshelf](https://palace-bookshelf-opds2.dp.la/v1/publications) is a good example of such a feed. Enter the
following details:

![.github/readme/collection.png](.github/readme/collection.png)

Note that we associate the collection with the _Hazelnut Peak_ library by selecting it in the `Libraries` drop-down.
A collection can be associated with any number of libraries.

##### Importing

At this point, we have a library named _Hazelnut Peak_ configured to use the _Palace Bookshelf_ collection we created.
It's now necessary to tell the application to start importing books from the OPDS feed. When the application is
running inside a Docker image, the image is typically configured to execute various import operations on a regular
schedule using `cron`. Because we're running the application from the command-line for development purposes, we
need to execute these operations ourselves manually. In this particular case, we need to execute the `opds_import_monitor`:

```sh
(circ) $ ./bin/opds_import_monitor
{"host": "hazelnut",
 "app": "simplified",
 "name": "OPDS Import Monitor",
 "level": "INFO",
 "filename": "opds_import.py",
 "message": "[Palace Bookshelf] Following next link: http://openbookshelf.dp.la/lists/Open%20Bookshelf/crawlable",
 "timestamp": "2022-01-17T11:52:35.839978+00:00"}
...
```

The command will cause the application to crawl the configured OPDS feed and import every book in it. At the time
of writing, this command will take around an hour to run the first time it is executed, but subsequent executions
complete in seconds. Please wait for the import to complete before continuing!

When the import has completed, the books are imported but no OPDS feeds will have been generated, and no search
service has been configured.

#### Configuring Search

Navigate to `System Configuration → Search` and add a new search configuration. The required URL is
the URL of the [Opensearch instance configured earlier](#opensearch):

![Opensearch](.github/readme/search.png)

#### Generating Search Indices

As with the collection [configured earlier](#adding-collections), the application depends upon various operations
being executed on a regular schedule to generate search indices. Because we're running the application from
the local command-line, we need to execute those operations manually:

```sh
./bin/search_index_clear
./bin/search_index_refresh
```

Neither of the commands will produce any output if the operations succeed.

#### Generating OPDS Feeds

When the collection has finished [importing](#importing), we are required to generate OPDS feeds. Again,
this operation is configured to execute on a regular schedule in the Docker image, but we'll need to execute
it manually here:

```sh
./bin/opds_entry_coverage
```

The command will produce output indicating any errors.

Navigating to `http://localhost:6500/` should show an OPDS feed containing various books:

![Feed](.github/readme/feed.png)

#### Troubleshooting

The `./bin/repair/where_are_my_books` command can produce output that may indicate why books are not appearing
in OPDS feeds. A working, correctly configured installation, at the time of writing, produces output such as this:

```sh
(circ) $ ./bin/repair/where_are_my_books
Checking library Hazelnut Peak
 Associated with collection Palace Bookshelf.
 Associated with 171 lanes.

0 feeds in cachedfeeds table, not counting grouped feeds.

Examining collection "Palace Bookshelf"
 7838 presentation-ready works.
 0 works not presentation-ready.
 7824 works in the search index, expected around 7838.
```

We can see from the above output that the vast majority of the books in the _Open Bookshelf_ collection
were indexed correctly.

### Sitewide Settings

Some settings have been provided in the admin UI that configure or toggle various functions of the Circulation Manager.
These can be found at `/admin/web/config/SitewideSettings` in the admin interface.

#### Push Notification Status

This setting is a toggle that may be used to turn on or off the ability for the the system
to send the Loan and Hold reminders to the mobile applications.

## Scheduled Jobs

The Palace Manager has a number of background jobs that are scheduled to run at regular intervals. This
includes all the import and reaper jobs, as well as other necessary background tasks, such as maintaining
the search index and feed caches.

Jobs are scheduled via a combination of `cron` and `celery`. All new jobs should use `celery` for scheduling,
and existing jobs are being migrated to `celery` as they are updated.

The `cron` jobs are defined in the `docker/services/simplified_crontab` file. The `celery` jobs are defined
in the `core/celery/tasks/` module.

## Code Style

Code style on this project is linted using [pre-commit](https://pre-commit.com/). This python application is included
in our `pyproject.toml` file, so if you have the applications requirements installed it should be available. pre-commit
is run automatically on each push and PR by our [CI System](#continuous-integration).

You can run it manually on all files with the command: `pre-commit run --all-files`.

You can also set it up, so that it runs automatically for you on each commit. Running the command `pre-commit install`
will install the pre-commit script in your local repositories git hooks folder, so that pre-commit is run automatically
on each commit.

### Configuration

The pre-commit configuration file is named [`.pre-commit-config.yaml`](.pre-commit-config.yaml). This file configures
the different lints that pre-commit runs.

### Linters

#### Built in

Pre-commit ships with a [number of lints](https://pre-commit.com/hooks.html) out of the box, we are configured to use:
- `trailing-whitespace` - trims trailing whitespace.
- `end-of-file-fixer` - ensures that a file is either empty, or ends with one newline.
- `check-yaml` - checks yaml files for parseable syntax.
- `check-json` - checks json files for parseable syntax.
- `check-ast` - simply checks whether the files parse as valid python.
- `check-shebang-scripts-are-executable` - ensures that (non-binary) files with a shebang are executable.
- `check-executables-have-shebangs` -  ensures that (non-binary) executables have a shebang.
- `check-merge-conflict` - checks for files that contain merge conflict strings.
- `check-added-large-files` - prevents giant files from being committed.
- `mixed-line-ending` - replaces or checks mixed line ending.

#### Black

We lint using the [black](https://black.readthedocs.io/en/stable/) code formatter, so that all of our code is formatted
consistently.

#### isort

We lint to make sure our imports are sorted and correctly formatted using [isort](https://pycqa.github.io/isort/). Our
isort configuration is stored in our [tox.ini](tox.ini) which isort automatically detects.

#### autoflake

We lint using [autoflake](https://pypi.org/project/autoflake/) to flag and remove any unused import statement. If an
unused import is needed for some reason it can be ignored with a `#noqa` comment in the code.

## Continuous Integration

This project runs all the unit tests through Github Actions for new pull requests and when merging into the default
`main` branch. The relevant file can be found in `.github/workflows/test-build.yml`. When contributing updates or
fixes, it's required for the test Github Action to pass for all Python 3 environments. Run the `tox` command locally
before pushing changes to make sure you find any failing tests before committing them.

For each push to a branch, CI also creates a docker image for the code in the branch. These images can be used for
testing the branch, or deploying hotfixes.

To install the tools used by CI run:

```sh
poetry install --only ci
```

## Testing

The Github Actions CI service runs the unit tests against Python 3.11, and 3.12 automatically using
[tox](https://tox.readthedocs.io/en/latest/).

Tox has an environment for each python version, the module being tested, and an optional `-docker` factor that will
automatically use docker to deploy service containers used for the tests. You can select the environment you would like
to test with the tox `-e` flag.

### Factors

When running tox without an environment specified, it tests using all supported Python versions
with service dependencies running in docker containers.

#### Python version

| Factor | Python Version |
|--------|----------------|
| py311  | Python 3.11    |
| py312  | Python 3.12    |

All of these environments are tested by default when running tox. To test one specific environment you can use the `-e`
flag.

Test Python 3.11

```sh
tox -e py311
```

You need to have the Python versions you are testing against installed on your local system. `tox` searches the system
for installed Python versions, but does not install new Python versions. If `tox` doesn't find the Python version its
looking for it will give an `InterpreterNotFound` errror.

[Pyenv](#pyenv) is a useful tool to install multiple Python versions, if you need to install
missing Python versions in your system for local testing.

#### Docker

If you install `tox-docker` tox will take care of setting up all the service containers necessary to run the unit tests
and pass the correct environment variables to configure the tests to use these services. Using `tox-docker` is not
required, but it is the recommended way to run the tests locally, since it runs the tests in the same way they are run
on the Github Actions CI server. `tox-docker` is automatically included when installing the `ci` dependency group.

The docker functionality is included in a `docker` factor that can be added to the environment. To run an environment
with a particular factor you add it to the end of the environment.

Test with Python 3.11 using docker containers for the services.

```sh
tox -e "py311-docker"
```

### Local services

If you already have elastic search or postgres running locally, you can run them instead by setting the
following environment variables:

- `PALACE_TEST_DATABASE_URL`
- `PALACE_TEST_SEARCH_URL`

Make sure the ports and usernames are updated to reflect the local configuration.

```sh
# Set environment variables
export PALACE_TEST_DATABASE_URL="postgresql://simplified_test:test@localhost:9005/simplified_circulation_test"
export PALACE_TEST_SEARCH_URL="http://localhost:9200"

# Run tox
tox -e "py311"
```

The tests assume that they have permission to create and drop databases. They connect to the
provided database URL and create a new database for each test run. If the user does not have permission
to create and drop databases, the tests will fail. You can disable this behavior by setting the
`PALACE_TEST_DATABASE_CREATE_DATABASE` environment variable to `false`.

```sh

### Override `pytest` arguments

If you wish to pass additional arguments to `pytest` you can do so through `tox`. Every argument passed after a `--` to
the `tox` command line will the passed to `pytest`, overriding the default.

Only run the `service` tests with Python 3.11 using docker.

```sh
tox -e "py311-docker" -- tests/manager/service
```

### Environment Variables

When testing Celery tasks, it can be useful to set the `PALACE_TEST_CELERY_WORKER_SHUTDOWN_TIMEOUT` environment variable
to a higher value than the default of 30 seconds, so you are able to set breakpoints in the worker code and debug it.
This value is interpreted as the number of seconds to wait for the worker to shut down before killing it. If you set
this value to `none` or (empty string), timeouts will be disabled.

```sh
export PALACE_TEST_CELERY_WORKER_SHUTDOWN_TIMEOUT=""
```

### Coverage Reports

Code coverage is automatically tracked with [`pytest-cov`](https://pypi.org/project/pytest-cov/) when tests are run.
When the tests are run with github actions, the coverage report is automatically uploaded to
[codecov](https://about.codecov.io/) and the results are added to the relevant pull request.

When running locally, the results from each individual run can be collected and combined into an HTML report using
the `report` tox environment. This can be run on its own after running the tests, or as part of the tox environment
selection.

```shell
# Run core and api tests under Python 3.8, using docker
# containers for dependencies, and generate code coverage report
tox -e "py38-{core,api}-docker,report"
```

## Usage with Docker

Check out the [Docker README](/docker/README.md) in the `/docker` directory for in-depth information on optionally
running and developing the Circulation Manager locally with Docker, or for deploying the Circulation Manager with
Docker.

## Performance Profiling

There are three different profilers included to help measure the performance of the application. They can each be
enabled by setting environment variables while starting the application.

### AWS XRay

#### Environment Variables

- `PALACE_XRAY`: Set to enable X-Ray tracing on the application.
- `PALACE_XRAY_NAME`: The name of the service shown in x-ray for these traces.
- `PALACE_XRAY_ANNOTATE_`: Any environment variable starting with this prefix will be added to to the trace as an
  annotation.
    - For example setting `PALACE_XRAY_ANNOTATE_KEY=value` will set the annotation `key=value` on all xray traces sent
    from the application.
- `PALACE_XRAY_INCLUDE_BARCODE`: If this environment variable is set to `true` then the tracing code will try to include
  the patrons barcode in the user parameter of the trace, if a barcode is available.

Additional environment variables are provided by the
[X-Ray Python SDK](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-python-configuration.html#xray-sdk-python-configuration-envvars).

### cProfile

This profiler uses the
[werkzeug `ProfilerMiddleware`](https://werkzeug.palletsprojects.com/en/2.0.x/middleware/profiler/)
to profile the code. This uses the
[cProfile](https://docs.python.org/3/library/profile.html#module-cProfile)
module under the hood to do the profiling.

#### Environment Variables

- `PALACE_CPROFILE`: Profiling will the enabled if this variable is set. The saved profile data will be available at
  path specified in the environment variable.
- The profile data will have the extension `.prof`.
- The data can be accessed using the
[`pstats.Stats` class](https://docs.python.org/3/library/profile.html#the-stats-class).
- Example code to print details of the gathered statistics:
  ```python
  import os
  from pathlib import Path
  from pstats import SortKey, Stats

  path = Path(os.environ.get("PALACE_CPROFILE"))
  for file in path.glob("*.prof"):
      stats = Stats(str(file))
      stats.sort_stats(SortKey.CUMULATIVE, SortKey.CALLS)
      stats.print_stats()
  ```

### PyInstrument

This profiler uses [PyInstrument](https://pyinstrument.readthedocs.io/en/latest/) to profile the code.

#### Profiling tests suite

PyInstrument can also be used to profile the test suite. This can be useful to identify slow tests, or to identify
performance regressions.

To profile the test suite, run the following command:

```sh
pyinstrument -m pytest --no-cov -n 0 tests
```

#### Environment Variables

- `PALACE_PYINSTRUMENT`: Profiling will the enabled if this variable is set. The saved profile data will be available at
  path specified in the environment variable.
    - The profile data will have the extension `.pyisession`.
    - The data can be accessed with the
    [`pyinstrument.session.Session` class](https://pyinstrument.readthedocs.io/en/latest/reference.html#pyinstrument.session.Session).
    - Example code to print details of the gathered statistics:
      ```python
      import os
      from pathlib import Path

      from pyinstrument.renderers import HTMLRenderer
      from pyinstrument.session import Session

      path = Path(os.environ.get("PALACE_PYINSTRUMENT"))
      for file in path.glob("*.pyisession"):
          session = Session.load(file)
          renderer = HTMLRenderer()
          renderer.open_in_browser(session)
      ```

### Other Environment Variables

- `SIMPLIFIED_SIRSI_DYNIX_APP_ID`: The Application ID for the SirsiDynix Authentication API (optional)
