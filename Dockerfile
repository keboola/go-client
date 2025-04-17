FROM golang:1.24.1

ENV HOME=/my-home
ENV GOCACHE=/tmp/cache/go
ENV GOMODCACHE=/tmp/cache/go-mod
ENV GOFLAGS="-mod=mod"
ENV PATH="$PATH:$GOPATH/bin"

# Install editor
RUN apt-get update && apt-get install -y nano
ENV EDITOR=nano

# Install Task
RUN sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d

# Install tools
RUN mkdir -p /tmp/build
COPY Taskfile.yml /tmp/build/Taskfile.yml
COPY scripts  /tmp/build/scripts
RUN cd /tmp/build && task tools && rm -rf /tmp/build

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
