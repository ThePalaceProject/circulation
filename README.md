# Library Simplified Circulation Manager
[![Test Circulation & Build Docker Images](https://github.com/lyrasis/simplye-circulation/actions/workflows/test-build.yml/badge.svg)](https://github.com/lyrasis/simplye-circulation/actions/workflows/test-build.yml)

This is a [LYRASIS](http://lyrasis.org) maintained fork of the NYPL [Library Simplified](http://www.librarysimplified.org/) Circulation Manager.

It depends on [Server Core](https://github.com/lyrasis/simplye-server-core) as a git submodule.

## Installation

Docker images created from this code are available at:
https://hub.docker.com/repository/docker/lyrasis/circ-webapp
https://hub.docker.com/repository/docker/lyrasis/circ-scripts

## Git Branch Workflow

| Branch   | Python Version |
| -------- | -------------- |
| develop  | Python 3       |
| main     | Python 3       |
| python2  | Python 2       |

The default branch is `develop` and that's the working branch that should be used when branching off for bug fixes or new features. Once a feature branch pull request is merged into `develop`, the changes can be merged to `main` to create releases.

Python 2 stopped being supported after January 1st, 2020 but there is still a `python2` branch which can be used. As of May 2021, development will be done in the `develop` and `main` branches.

There are additional protected branches that are used for *NYPL-specific* deployments to keep in mind.

| Branch          |
| --------------- |
| nypl-deploy-qa  |
| nypl-deploy-production  |
| openebooks-deploy-qa  |
| openebooks-deploy-qa  |
| bpl-deploy-qa  |
| bpl-deploy-production  |

### Python setup

This project uses python 3 for development. You will need to set up a local virtual environment to install packages and run the project. Start by creating the virtual environment:

```sh
$ python3 -m venv env
```

Then include the database URLS as environment variables at the end in `/env/bin/activate`. These databases should be created before this step and more information can be found in the [Library Simplified wiki](https://github.com/NYPL-Simplified/Simplified/wiki/Deployment-Instructions):

```
export SIMPLIFIED_PRODUCTION_DATABASE="postgres://simplified:[password]@localhost:5432/simplified_circulation_dev"
export SIMPLIFIED_TEST_DATABASE="postgres://simplified_test:[password]@localhost:5432/simplified_circulation_test"
```

Activate the virtual environment:

```sh
$ source env/bin/activate
```

and install the dependencies:

```sh
$ pip install -r requirements-dev.txt
```

## Generating Documentation

Code documentation can be generated using Sphinx. The configuration for the documentation can be found in `/docs`.

Github Actions handles generating the `.rst` source files, generating the HTML static site, and deploying the build to the `gh-pages` branch.

To view the documentation _locally_, go into the `/docs` directory and run `make html`. This will generate the .rst source files and build the static site in `/docs/build/html`

## Continuous Integration

This project runs all the unit tests through Github Actions for new pull requests and when merging into the default `develop` branch. The relevant file can be found in `.github/workflows/test.yml`. When contributing updates or fixes, it's required for the test Github Action to pass for all python 3 environments. Run the `tox` command locally before pushing changes to make sure you find any failing tests before committing them.

As mentioned above, Github Actions is also used to build and deploy Sphinx documentation to Github Pages. The relevant file can be found in `.github/workflows/docks.yml`.

## Testing
The Github Actions CI service runs the unit tests against Python 3.6, 3.7, and 3.8 automatically using [tox](https://tox.readthedocs.io/en/latest/).

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
