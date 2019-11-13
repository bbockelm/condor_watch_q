@echo off

SET CONTAINER_TAG=condor_watch_q

ECHO Building condor_watch_q testing container...

docker build --quiet -t %CONTAINER_TAG% .

docker run -it --rm --mount type=bind,source="%CD%",target=/home/watcher/condor_watch_q,readonly %CONTAINER_TAG% %*
