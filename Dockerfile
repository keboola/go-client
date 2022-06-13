FROM golang:1.18

ENV HOME=/my-home
ENV GOCACHE=/tmp/cache/go
ENV GOMODCACHE=/tmp/cache/go-mod
ENV GOFLAGS="-mod=mod"
ENV PATH="$PATH:$GOPATH/bin"

# Install editor
RUN apt-get update && apt-get install -y nano
ENV EDITOR=nano

# Install tools
RUN mkdir -p /tmp/build
COPY Makefile /tmp/build/Makefile
COPY scripts  /tmp/build/scripts
RUN cd /tmp/build && make install-tools && rm -rf /tmp/build

# Set prompt
RUN mkdir -p ~ && \
    echo 'PS1="\w > "' > ~/.bashrc

# Fix permissions
RUN mkdir -p $GOPATH && chmod -R 777 $GOPATH && \
    mkdir -p $GOCACHE && chmod -R 777 $GOCACHE && \
    mkdir -p $GOMODCACHE && chmod -R 777 $GOMODCACHE && \
    mkdir -p $HOME && chmod -R 777 $HOME

WORKDIR /code/
CMD ["/bin/bash"]
