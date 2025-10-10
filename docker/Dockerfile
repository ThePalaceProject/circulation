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

# Setup logrotate
COPY --chmod=644 docker/services/logrotate /etc/

# Copy our poetry files into the image and install our dependencies.
COPY --chown=palace:palace poetry.lock pyproject.toml /var/www/circulation/
RUN . env/bin/activate && \
    poetry sync --only main,pg --no-root

COPY --chown=palace:palace . /var/www/circulation

RUN . env/bin/activate && \
    poetry install --only-root

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
