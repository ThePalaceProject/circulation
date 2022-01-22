###############################################################################
## lcpencrypt
###############################################################################

FROM amd64/golang AS builder

LABEL maintainer="The Palace Project <info@thepalaceproject.org>"

RUN go get -v github.com/readium/readium-lcp-server/lcpencrypt

###############################################################################
## Final image
###############################################################################

FROM phusion/baseimage:focal-1.0.0 As base

# Copy over all Palace build files for this image
COPY ./docker /ls_build
COPY --chown=1000:1000 . /var/www/circulation

RUN /bin/bash -c "/ls_build/simplified_app.sh \
      && /ls_build/logrotate.sh"

FROM base as exec

COPY --from=builder /go/bin/lcpencrypt /go/bin/lcpencrypt

ENV SIMPLIFIED_DB_TASK "ignore"
ENV SIMPLIFIED_SCRIPT_NAME ""

RUN /bin/bash -c "rm -rf /ls_build && /bd_build/cleanup.sh"

VOLUME /var/log
WORKDIR /home/simplified/circulation/bin
CMD ["/sbin/my_init", "--skip-runit", "--quiet", "--", \
     "/bin/bash", "-c", \
     "source ../env/bin/activate && ./${SIMPLIFIED_SCRIPT_NAME}"]

FROM base as scripts

COPY --from=builder /go/bin/lcpencrypt /go/bin/lcpencrypt

ENV SIMPLIFIED_DB_TASK "auto"
# Set the local timezone in /docker/simplified_cron.sh
ENV TZ=US/Eastern

RUN /bin/bash -c "/ls_build/simplified_cron.sh \
      && rm -rf /ls_build && /bd_build/cleanup.sh"

VOLUME /var/log
WORKDIR /home/simplified/circulation/bin

CMD ["/sbin/my_init"]

FROM base as webapp

ENV SIMPLIFIED_DB_TASK "ignore"
ENV LANG=C.UTF-8

RUN /bin/bash -c "/ls_build/nginx.sh \
      && /ls_build/uwsgi.sh \
      && rm -rf /ls_build && /bd_build/cleanup.sh"

VOLUME /var/log
WORKDIR /home/simplified/circulation
EXPOSE 80

CMD ["/sbin/my_init"]
