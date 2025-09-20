#!/bin/bash

# Step 1: Clone Git repository
git clone git@gitlab.engr.illinois.edu:saik2/mp1-g02.git
cd mp1-g02
go mod init mp1-g02
go mod tidy
chmod +x ./scripts/*.sh

host=$(hostname)
num=${host#fa25-cs425-02}
num=${num%%.*}
num=$((10#$num))

touch machine.$num.log

# Step 2: Install air for automatic reloading
go install github.com/air-verse/air@latest
EXPORT_COMMAND="export PATH="$(go env GOPATH)/bin:$PATH""
echo "$EXPORT_COMMAND" >> ~/.bashrc
source ~/.bashrc
echo $PWD
cat > .air.toml << 'EOF'
root = "."
tmp_dir = "tmp"

[build]
  args_bin = []
  bin = "./tmp/server.exe"
  cmd = "go build -o ./tmp/server.exe ./server/server.go"
  exclude_dir = ["tmp"]
  full_bin = ""
  include_dir = ["server", "client"]
  include_ext = ["go", "tpl", "tmpl", "html"]
  include_file = []
  log = "build-errors.log"
  rerun = true
  send_interrupt = false
  stop_on_error = false

[screen]
  clear_on_rebuild = false
  keep_scroll = true
EOF

air -c .air.toml