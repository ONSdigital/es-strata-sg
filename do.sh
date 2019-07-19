#!/bin/bash

COLUMNS="`tput cols`"
LINES="`tput lines`"
BOLD=$(tput bold)
NORMAL=$(tput sgr0)

DOCKER_COMPOSE="docker-compose -f docker-compose.yml"

if [[ -e "docker-compose.override.yml" ]]; then
    # This file is local/optional and NOT checked in.
    DOCKER_COMPOSE="$DOCKER_COMPOSE -f docker-compose.override.yml"
fi

os=${OSTYPE//[0-9.-]*/}
case "$os" in
  darwin)
    ;;

  msys)
    DOCKER_COMPOSE="winpty $DOCKER_COMPOSE"
    ;;

  linux)
    ;;
  *)

  echo "Unsupported operating system $OSTYPE"
  exit 1
esac

_requires() {
    service="$1"
    $DOCKER_COMPOSE ps -q $service &> /dev/null
    if [[ "$?" == 1 ]]; then
        echo "'$service' service is not running. Please run \`start\` first."
        exit 1
    fi
}

build() {
    $DOCKER_COMPOSE build --force-rm "${@:3}"
}

compose() {
    $DOCKER_COMPOSE "$@"
}

init() {
   run serverless serverless create "$@" 
}

test() {
   run python py.test "$@" 
}

shell() {
    run $@ /bin/bash
}

pyshell() {
    run $@ python ipython
}

exec() {
    $DOCKER_COMPOSE exec -e COLUMNS -e LINES "$@"
}

run() {
    $DOCKER_COMPOSE run --rm -e uid=$UID -w //usr/src/app "$@"
}

serverless() {
   run serverless serverless "$@"
}

_usage() {
    cat <<USAGE
Utility wrapper around docker-compose.

Usage:

    ${BOLD}build${NORMAL} [<arg>]

        Builds all the images (or the ones specified).

        Ex:
            ./do.sh build

    ${BOLD}run${NORMAL} [<arg>]

        Bring up a container, execute a command in the container, removes the container after the command has finished.

    ${BOLD}check${NORMAL}

        Checks python code and dependencies for issues.

    ${BOLD}shell${NORMAL}
        Brings up a container, runs a shell, removes container once shell exits.

    ${BOLD}pyshell${NORMAL}

        Brings up a container, opens an ipython shell, removed container once shell exits.

    ${BOLD}test${NORMAL}

        Brings up python container, run backend tests using pytest, container removed once tests have finished. 

USAGE
}

if [ "$1" == "" ]; then
    _usage
fi

$*
