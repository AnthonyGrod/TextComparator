FROM ubuntu:latest
LABEL authors="antonigrodowski"

ENTRYPOINT ["top", "-b"]