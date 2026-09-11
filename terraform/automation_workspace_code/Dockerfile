FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y curl git jq unzip

RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

WORKDIR /actions-runner

RUN curl -o actions-runner.tar.gz -L \
https://github.com/actions/runner/releases/download/v2.337.0/actions-runner-linux-x64-2.337.0.tar.gz

RUN tar xzf actions-runner.tar.gz

COPY start.sh .

RUN chmod +x start.sh

ENTRYPOINT ["./start.sh"]