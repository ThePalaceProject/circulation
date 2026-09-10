###############################################################################
# Common build step
#
# This is a multi-stage build, so we can use the same build steps for
# both the webapp and the scripts images, since they are largely the same.
# The only difference are what services are enabled.
###############################################################################

ARG BASE_IMAGE=ghcr.io/thepalaceproject/circ-baseimage:latest

FROM ${BASE_IMAGE} AS common

# Copy startup scripts
COPY docker/startup /etc/my_init.d/

# Setup logrotate and run it hourly instead of daily
COPY --chmod=644 docker/services/logrotate /etc/
RUN mv /etc/cron.daily/logrotate /etc/cron.hourly/logrotate

# Copy our uv files into the image and install our dependencies.
# We copy the workspace-member pyproject.toml files (but not their source) so that
# uv can resolve workspace members without invalidating this layer on source changes.
COPY --chown=palace:palace uv.lock pyproject.toml /var/www/circulation/
COPY --chown=palace:palace packages/palace-brainfuck-utils/pyproject.toml /var/www/circulation/packages/palace-brainfuck-utils/
COPY --chown=palace:palace packages/palace-brainfuck-vm/pyproject.toml /var/www/circulation/packages/palace-brainfuck-vm/
COPY --chown=palace:palace packages/palace-util/pyproject.toml /var/www/circulation/packages/palace-util/
COPY --chown=palace:palace packages/palace-opds/pyproject.toml /var/www/circulation/packages/palace-opds/
RUN uv sync --frozen --no-dev --no-install-workspace

COPY --chown=palace:palace . /var/www/circulation

# Install the project itself as an editable install.
#
# palace-brainfuck-utils depends on palace-brainfuck-vm, a Rust extension that
# uv builds from source via maturin, so a toolchain has to be on PATH for this
# step. Ubuntu's apt-provided rustc is too old for that crate's 2024 edition,
# so we take it from rustup. It is installed and removed inside this one layer
# so that the compiler and its ~1GB of toolchain do not ship in the runtime
# image; only the compiled extension module survives.
RUN export RUSTUP_HOME=/tmp/rustup CARGO_HOME=/tmp/cargo && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
      sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path && \
    PATH="$CARGO_HOME/bin:$PATH" uv sync --frozen --no-dev && \
    rm -rf "$RUSTUP_HOME" "$CARGO_HOME" \
           packages/palace-brainfuck-vm/target /root/.cache

###############################################################################
## Circ-exec Image
###############################################################################

FROM common AS exec

ENV SIMPLIFIED_SCRIPT_NAME=""

VOLUME /var/log
WORKDIR /home/palace/circulation/bin
CMD ["/sbin/my_init", "--skip-runit", "--quiet", "--", \
     "/bin/bash", "-c", \
     "source ../env/bin/activate && ./${SIMPLIFIED_SCRIPT_NAME}"]

###############################################################################
## Circ-scripts Image
###############################################################################

FROM common AS scripts

# Set the local timezone and create the cron log file
ENV TZ=US/Eastern
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    touch /var/log/cron.log

# Setup runit
COPY docker/runit-scripts /etc/service/

VOLUME /var/log
WORKDIR /home/palace/circulation/bin

CMD ["/sbin/my_init"]

###############################################################################
## Circ-webapp Image
###############################################################################

FROM common AS webapp

# Setup nginx
COPY docker/services/nginx /etc/nginx/

# Setup uwsgi
COPY docker/services/uwsgi /etc/
RUN mkdir /var/run/uwsgi && \
    chown palace:palace /var/run/uwsgi

# Setup runit
COPY docker/runit-web /etc/service/

VOLUME /var/log
WORKDIR /home/palace/circulation
EXPOSE 80

CMD ["/sbin/my_init"]
