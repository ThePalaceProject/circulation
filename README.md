# Palace Manager
[![Test & Build](https://github.com/ThePalaceProject/circulation/actions/workflows/test-build.yml/badge.svg)](https://github.com/ThePalaceProject/circulation/actions/workflows/test-build.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
![Python: 3.6,3.7,3.8,3.9](https://img.shields.io/badge/Python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9-blue)

This is a [The Palace Project](https://thepalaceproject.org) maintained fork of the NYPL [Library Simplified](http://www.librarysimplified.org/) Circulation Manager.

It depends on [Circulation Core](https://github.com/thepalaceproject/circulation-core) as a git submodule.

## Installation

Docker images created from this code are available at:
https://github.com/ThePalaceProject/circulation/pkgs/container/circ-webapp
https://github.com/ThePalaceProject/circulation/pkgs/container/circ-scripts

## Git Branch Workflow

| Branch   | Python Version |
| -------- | -------------- |
| main     | Python 3       |
| python2  | Python 2       |

The default branch is `main` and that's the working branch that should be used when branching off for bug fixes or new features.

Python 2 stopped being supported after January 1st, 2020 but there is still a `python2` branch which can be used. As of August 2021, development will be done in the `main` branch and the `python2` branch will not be updated unless absolutely necessary.

## Set Up

### Python Set Up

If you do not have Python 3 installed, you can use [Homebrew](https://brew.sh/)* to install it by running the command `$ brew install python3`.

*If you do not yet have Homebrew, you can install it by running the following:

```
$ /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```
While you're at it, go ahead and install the following required dependencies:

* `$ brew install pkg-config libffi`
* `$ brew install libxmlsec1`
* `$ brew install libjpeg`

Please note: only certain versions of Python 3 will work with this application. One such version is Python 3.6.5. Check to see which version you currently have installed by running `$ python -V`.

If you're using a version of Python that doesn't work, install [pyenv](https://github.com/pyenv/pyenv-installer) using command `$ curl https://pyenv.run | bash`, then install the version of Python you want to work with, ie `$ pyenv install python3.6.5`, and then run `$ pyenv global 3.6.5`. Check the current version again with `$ python -V` to make sure it's correct before proceeding.

You will need to set up a local virtual environment to install packages and run the project. If you haven't done so before, use pip to install virtualenv – `$ pip install virtualenv` – before creating the virtual environment in the root of the circulation repository:

```sh
$ python -m venv env
```

As mentioned above, this application depends on [LCirculation Core](https://github.com/thepalaceproject/circulation-core) as a git submodule. To set that up, in the repository, run:

* `$ git submodule init`
* `$ git submodule update`

### Elasticsearch Set Up

The circulation manager requires Elasticsearch. If you don't have Elasticsearch, check out instructions in the [Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions), or simply read on.

1. Download it [here](https://www.elastic.co/downloads/past-releases/elasticsearch-6-8-6).
2. `cd` into the `elasticsearch-[version number]` directory.
3. Run `$ elasticsearch-plugin install analysis-icu`
4. Run `$ ./bin/elasticsearch`.
5. You may be prompted to download [Java SE](https://www.oracle.com/java/technologies/javase-downloads.html). If so, go ahead and do so.
6. Check `http://localhost:9200` to make sure the Elasticsearch server is running.

### Database Set Up

The databases should be created next. To find instructions for how to do so, check out the [Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions), or simply read on.

1. Download and install [Postgres](https://www.postgresql.org/download/) if you don't have it already.
2. Use the command `$ psql` to access the Postgresql client.
3. Within the session, run the following commands, adding your own password in lieu of the [password] placeholders:
```sh
CREATE DATABASE simplified_circulation_test;
CREATE DATABASE simplified_circulation_dev;

CREATE USER simplified with password '[password]';
grant all privileges on database simplified_circulation_dev to simplified;

CREATE USER simplified_test with password '[password]';
grant all privileges on database simplified_circulation_test to simplified_test;

--Add pgcrypto to any circulation manager databases.
\c simplified_circulation_dev
create extension pgcrypto;
\c simplified_circulation_test
create extension pgcrypto;
```

Then, add the following database URLS as environment variables at the end of the `/env/bin/activate` file within the circulation repo, including the password you created earlier:

```
export SIMPLIFIED_PRODUCTION_DATABASE="postgres://simplified:[password]@localhost:5432/simplified_circulation_dev"
export SIMPLIFIED_TEST_DATABASE="postgres://simplified_test:[password]@localhost:5432/simplified_circulation_test"
```

### Running the Application

Activate the virtual environment:

```sh
$ source env/bin/activate
```

And install the dependencies:

```sh
$ pip install -r requirements-dev.txt
```

Run the application with:

```sh
$ python app.py
```
And visit `http://localhost:6500/`.

### Python Installation Issues

When running the `pip install ...` command, you may run into installation issues. The [Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions) instructions say to install some packages through brew such as `libxmlsec1`. On newer macos machines, you may encounter an error such as:

```sh
error: command '/usr/bin/clang' failed with exit code 1
  ----------------------------------------
  ERROR: Failed building wheel for xmlsec
Failed to build xmlsec
ERROR: Could not build wheels for xmlsec which use PEP 517 and cannot be installed directly
```

This typically happens after installing packages through brew and then running the `pip install` command.

This [blog post](https://mbbroberg.fun/clang-error-in-pip/) explains and shows a fix for this issue. Start by trying the `xcode-select --install` command. If it does not work, you can try adding the following to your `~/.zshrc` or `~/.bashrc` file, depending on what you use:

```sh
export CPPFLAGS="-DXMLSEC_NO_XKMS=1"
```

## Generating Documentation

Code documentation can be generated using Sphinx. The configuration for the documentation can be found in `/docs`.

Github Actions handles generating the `.rst` source files, generating the HTML static site, and deploying the build to the `gh-pages` branch.

To view the documentation _locally_, go into the `/docs` directory and run `make html`. This will generate the .rst source files and build the static site in `/docs/build/html`

## Code Style

Code style on this project is linted using [pre-commit](https://pre-commit.com/). This python application is included in our `requirements-dev.txt` file, so if you have
the applications requirements installed it should be available. pre-commit is run automatically on each push and PR by our [CI System](#continuous-integration).

You can run it manually on all files with the command: `pre-commit run --all-files`.

You can also set it up, so that it runs automatically for you on each commit. Running the command `pre-commit install` will install the pre-commit script in your
local repositories git hooks folder, so that pre-commit is run automatically on each commit.

### Configuration

The pre-commit configuration file is named [`.pre-commit-config.yaml`](.pre-commit-config.yaml). This file configures the differnet lints that pre-commit runs.

### Linters

#### Built in

Pre-commit ships with a [number of lints](https://pre-commit.com/hooks.html) out of the box, we are configured to use:
- `trailing-whitespace` - trims trailing whitespace.
- `end-of-file-fixer` - ensures that a file is either empty, or ends with one newline.
- `check-yaml` - checks yaml files for parseable syntax.
- `check-json` - checks json files for parseable syntax.
- `check-ast` - simply checks whether the files parse as valid python.
- `check-shebang-scripts-are-executable` - ensures that (non-binary) files with a shebang are executable.
- `check-merge-conflict` - checks for files that contain merge conflict strings.
- `check-added-large-files` - prevents giant files from being committed.
- `mixed-line-ending` - replaces or checks mixed line ending.
- `requirements-txt-fixer` - sorts entries in requirements.txt.

#### Black

We lint using the [black](https://black.readthedocs.io/en/stable/) code formatter, so that all of our code is formatted consistently.

#### isort

We lint to make sure our imports are sorted and correctly formatted using [isort](https://pycqa.github.io/isort/). Our
isort configuration is stored in our [tox.ini](tox.ini) which isort automatically detects.

## Continuous Integration

This project runs all the unit tests through Github Actions for new pull requests and when merging into the default `develop` branch. The relevant file can be found in `.github/workflows/test.yml`. When contributing updates or fixes, it's required for the test Github Action to pass for all python 3 environments. Run the `tox` command locally before pushing changes to make sure you find any failing tests before committing them.

As mentioned above, Github Actions is also used to build and deploy Sphinx documentation to Github Pages. The relevant file can be found in `.github/workflows/docks.yml`.

## Testing

The Github Actions CI service runs the unit tests against Python 3.6, 3.7, 3.8 and 3.9 automatically using [tox](https://tox.readthedocs.io/en/latest/).

To run `pytest` unit tests locally, install `tox`.

```sh
pip install tox
```

Tox has an environment for each python version and an optional `-docker` factor that will automatically use docker to
deploy service containers used for the tests. You can select the environment you would like to test with the tox `-e`
flag.

### Environments

| Environment | Python Version |
| ----------- | -------------- |
| py36        | Python 3.6     |
| py37        | Python 3.7     |
| py38        | Python 3.8     |
| py39        | Python 3.9     |

All of these environments are tested by default when running tox. To test one specific environment you can use the `-e`
flag.

Test Python 3.8
```
tox -e py38
```

You need to have the Python versions you are testing against installed on your local system. `tox` searches the system for installed Python versions, but does not install new Python versions. If `tox` doesn't find the Python version its looking for it will give an `InterpreterNotFound` errror.

[Pyenv](https://github.com/pyenv/pyenv) is a useful tool to install multiple Python versions, if you need to install missing Python versions in your system for local testing.

### Docker

If you install `tox-docker` tox will take care of setting up all the service containers necessary to run the unit tests
and pass the correct environment variables to configure the tests to use these services. Using `tox-docker` is not required, but it is the recommended way to run the tests locally, since it runs the tests in the same way they are run on the Github Actions CI server.

```
pip install tox-docker
```

The docker functionality is included in a `docker` factor that can be added to the environment. To run an environment
with a particular factor you add it to the end of the environment.

Test with Python 3.8 using docker containers for the services.
```
tox -e py38-docker
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
export SIMPLIFIED_TEST_ELASTICSEARCH="http://localhost:9006"

# Run tox
tox -e py38
```

### Override `pytest` arguments

If you wish to pass additional arguments to `pytest` you can do so through `tox`. The default argument passed to `pytest`
is `tests`, however you can override this. Every argument passed after a `--` to the `tox` command line will the passed
to `pytest`, overriding the default.

Only run the `test_cdn` tests with Python 3.6 using docker.

```sh
tox -e py36-docker -- tests/test_google_analytics_provider.py
```

## Usage with Docker

Check out the [Docker README](/docker/README.md) in the `/docker` directory for in-depth information on optionally running and developing the Circulation Manager locally with Docker, or for deploying the Circulation Manager with Docker.

## Performance Profiling

There are three different profilers included to help measure the performance of the application. They can each be
enabled by setting environment variables while starting the application.

### AWS XRay

*Environment Variables*
- `PALACE_XRAY`: Set to enable X-Ray tracing on the application.
- `PALACE_XRAY_NAME`: The name of the service shown in x-ray for these traces.
- `PALACE_XRAY_ANNOTATE_`: Any environment variable starting with this prefix will be added to to the trace as an
  annotation.
    - For example setting `PALACE_XRAY_ANNOTATE_KEY=value` will set the annotation `key=value` on all xray traces sent
      from the application.
- `PALACE_XRAY_INCLUDE_BARCODE`: If this environment variable is set to `true` then the tracing code will try to include
  the patrons barcode in the user parameter of the trace, if a barcode is available.


Additional environment variables are provided by the [X-Ray Python SDK](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-python-configuration.html#xray-sdk-python-configuration-envvars).

### cProfile

This profiler uses the [werkzeug `ProfilerMiddleware`](https://werkzeug.palletsprojects.com/en/2.0.x/middleware/profiler/)
to profile the code. This uses the [cProfile](https://docs.python.org/3/library/profile.html#module-cProfile) module
under the hood to do the profiling.

*Environment Variables*
- `PALACE_CPROFILE`: Profiling will the enabled if this variable is set. The saved profile data will be available at
  path specified in the environment variable.
    - The profile data will have the extension `.prof`.
    - The data can be accessed using the [`pstats.Stats` class](https://docs.python.org/3/library/profile.html#the-stats-class).
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

*Environment Variables*
- `PALACE_PYINSTRUMENT`: Profiling will the enabled if this variable is set. The saved profile data will be available at
  path specified in the environment variable.
    - The profile data will have the extension `.pyisession`.
    - The data can be accessed with the [`pyinstrument.session.Session` class](https://pyinstrument.readthedocs.io/en/latest/reference.html#pyinstrument.session.Session).
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
