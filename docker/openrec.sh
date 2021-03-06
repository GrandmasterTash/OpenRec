#!/bin/bash

# OpenRec will be creating files and folders - best to use the current user to achieve this
# Otherwise, running in docker typically means the docker account is used to own those folders.
export OPENREC_UID=${UID}
export OPENREC_GID=${GID}

# Steward is a console app - not a headless app so we run interactively.
docker-compose run openrec /steward '/etc/openrec/register.yml' pushgateway:9091