# Palace Manager

[![Test & Build](https://github.com/ThePalaceProject/circulation/actions/workflows/test-build.yml/badge.svg)](https://github.com/ThePalaceProject/circulation/actions/workflows/test-build.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
![Python: 3.7,3.8,3.9](https://img.shields.io/badge/Python-3.7%20%7C%203.8%20%7C%203.9-blue)

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

### Python Set Up

#### Homebrew (OSX)

If you do not have Python 3 installed, you can use [Homebrew](https://brew.sh/) to install it by running the command
`brew install python3`.

If you do not yet have Homebrew, you can install it by running the following:

```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

While you're at it, go ahead and install the following required dependencies:

```sh
brew install pkg-config libffi
brew install libxmlsec1
brew install libjpeg
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

### Elasticsearch

#### Docker

The easiest way to setup a local elasticsearch environment is to use docker.

```sh
docker run -d --name es -e discovery.type=single-node -p 9200:9200 elasticsearch:6.8.6
docker exec es elasticsearch-plugin -s install analysis-icu
docker restart es
```

#### Local

1. Download it [here](https://www.elastic.co/downloads/past-releases/elasticsearch-6-8-6).
2. `cd` into the `elasticsearch-[version number]` directory.
3. Run `$ elasticsearch-plugin install analysis-icu`
4. Run `$ ./bin/elasticsearch`.
5. You may be prompted to download [Java SE](https://www.oracle.com/java/technologies/javase-downloads.html). If so, go
   ahead and do so.
6. Check `http://localhost:9200` to make sure the Elasticsearch server is running.

### Database

#### Docker

```sh
docker run -d --name pg -e POSTGRES_USER=palace -e POSTGRES_PASSWORD=test -e POSTGRES_DB=circ -p 5432:5432 postgres:12
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

#### Environment variables

To let the application know which database to use set the `SIMPLIFIED_PRODUCTION_DATABASE` env variable.

```sh
export SIMPLIFIED_PRODUCTION_DATABASE="postgres://palace:test@localhost:5432/circ"
```

### Running the Application

As mentioned in the [pyenv](#pyenv) section, the `poetry` tool should be executed under a virtual environment
in order to guarantee that it will use the Python version you expect. To use a particular Python version,
you should create a local virtual environment in the cloned `circulation` repository directory. Assuming that
you want to use, for example, Python 3.9.9:

```sh
pyenv virtualenv 3.9.9 circ
```

This will create a new local virtual environment called `circ` that uses Python 3.9.9. Switch to that environment:

```sh
pyenv local circ
```

On most systems, using `pyenv` will adjust your shell prompt to indicate which virtual environment you
are now in. For example, the version of Python installed in your operating system might be `3.10.1`, but
using a virtual environment can substitute, for example, `3.9.9`:

```sh
$ python --version
Python 3.10.1

$ pyenv local circ
(circ) $ python --version
Python 3.9.9
```

For brevity, these instructions assume that all shell commands will be executed within a virtual environment.

Install the dependencies:

```sh
poetry install --no-root -E pg-binary
```

Run the application with:

```sh
poetry run python app.py
```

Check that there is now a web server listening on port `6500`:

```sh
curl http://localhost:6500/
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
is to use an open access OPDS feed as a data source. The [Open Bookshelf](https://openbookshelf.dp.la/) is a good example
of such a feed. Enter the following details:

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

Navigate to `System Configuration → Search` and add a new Elasticsearch configuration. The required URL is
the URL of the [Elasticsearch instance configured earlier](#elasticsearch):

![Elasticsearch](.github/readme/search.png)

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

### Installation Issues

When running the `poetry install ...` command, you may run into installation issues. On newer macos machines, you may
encounter an error such as:

```sh
error: command '/usr/bin/clang' failed with exit code 1
  ----------------------------------------
  ERROR: Failed building wheel for xmlsec
Failed to build xmlsec
ERROR: Could not build wheels for xmlsec which use PEP 517 and cannot be installed directly
```

This typically happens after installing packages through brew and then running the `pip install` command.

This [blog post](https://mbbroberg.fun/clang-error-in-pip/) explains and shows a fix for this issue. Start by trying
the `xcode-select --install` command. If it does not work, you can try adding the following to your `~/.zshrc` or
`~/.bashrc` file, depending on what you use:

```sh
export CPPFLAGS="-DXMLSEC_NO_XKMS=1"
```

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

## Testing

The Github Actions CI service runs the unit tests against Python 3.7, 3.8 and 3.9 automatically using
[tox](https://tox.readthedocs.io/en/latest/).

To run `pytest` unit tests locally, install `tox`.

```sh
pip install tox
```

Tox has an environment for each python version, the module being tested, and an optional `-docker` factor that will
automatically use docker to deploy service containers used for the tests. You can select the environment you would like
to test with the tox `-e` flag.

### Factors

When running tox without an environment specified, it tests `circulation` and `core` using all supported Python versions
with service dependencies running in docker containers.

#### Python version

| Factor      | Python Version |
| ----------- | -------------- |
| py37        | Python 3.7     |
| py38        | Python 3.8     |
| py39        | Python 3.9     |

All of these environments are tested by default when running tox. To test one specific environment you can use the `-e`
flag.

Test Python 3.8

```sh
tox -e py38
```

You need to have the Python versions you are testing against installed on your local system. `tox` searches the system
for installed Python versions, but does not install new Python versions. If `tox` doesn't find the Python version its
looking for it will give an `InterpreterNotFound` errror.

[Pyenv](#pyenv) is a useful tool to install multiple Python versions, if you need to install
missing Python versions in your system for local testing.

#### Module

| Factor      | Module            |
| ----------- | ----------------- |
| core        | core tests        |
| api         | api tests         |

#### Docker

If you install `tox-docker` tox will take care of setting up all the service containers necessary to run the unit tests
and pass the correct environment variables to configure the tests to use these services. Using `tox-docker` is not
required, but it is the recommended way to run the tests locally, since it runs the tests in the same way they are run
on the Github Actions CI server.

```sh
pip install tox-docker
```

The docker functionality is included in a `docker` factor that can be added to the environment. To run an environment
with a particular factor you add it to the end of the environment.

Test with Python 3.8 using docker containers for the services.

```sh
tox -e "py38-{api,core}-docker"
```

### Local services

If you already have elastic search or postgres running locally, you can run them instead by setting the
following environment variables:

- `SIMPLIFIED_TEST_DATABASE`
- `SIMPLIFIED_TEST_ELASTICSEARCH`

Make sure the ports and usernames are updated to reflect the local configuration.

```sh
# Set environment variables
export SIMPLIFIED_TEST_DATABASE="postgres://simplified_test:test@localhost:9005/simplified_circulation_test"
export SIMPLIFIED_TEST_ELASTICSEARCH="http://localhost:9200"

# Run tox
tox -e "py38-{api,core}"
```

### Override `pytest` arguments

If you wish to pass additional arguments to `pytest` you can do so through `tox`. Every argument passed after a `--` to
the `tox` command line will the passed to `pytest`, overriding the default.

Only run the `test_google_analytics_provider` tests with Python 3.8 using docker.

```sh
tox -e "py38-api-docker" -- tests/api/test_google_analytics_provider.py
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
