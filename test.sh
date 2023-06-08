#! /bin/bash

if [ "false" = "true" ]; then
  tag="ghcr.io/jonathangreen/circ-baseimage:main"
else
  tag="ghcr.io/jonathangreen/circ-baseimage:latest"
fi
echo tag="$tag"
