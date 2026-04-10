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
COPY --chown=palace:palace uv.lock pyproject.toml /var/www/circulation/
RUN UV_PROJECT_ENVIRONMENT=/var/www/circulation/env \
      uv sync --frozen --no-dev --no-install-project

COPY --chown=palace:palace . /var/www/circulation

# Install the project itself as a non-editable install. Omitting
# --no-install-project (and passing --no-editable) is the recommended
# pattern for production Docker builds with uv.
RUN UV_PROJECT_ENVIRONMENT=/var/www/circulation/env \
      uv sync --frozen --no-dev --no-editable

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

# Set the local timezone and setup cron
ENV TZ=US/Eastern
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    touch /var/log/cron.log

# Copy cron config into container
COPY docker/services/cron /etc/

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
