#!/bin/bash

# Step 1: Clone Git repository
git clone git@gitlab.engr.illinois.edu:saik2/mp2-g02.git
cd mp2-g02
go mod init mp2-g02
go mod tidy
chmod +x ./scripts/*.sh