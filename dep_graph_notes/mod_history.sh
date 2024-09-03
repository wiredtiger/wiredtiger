#!/usr/bin/env bash

# c4ff543d29f is roughly 2 years ago
start_commit=c4ff543d29fb4d02ba62f4349652477f46eb7b6f

# Checkout the script and create an original dep_file
git checkout modularity_check_script -- tools/modularity_check
git checkout "$start_commit"
./tools/modularity_check/modularity_check.py generate_dependency_file
cp ./tools/modularity_check/dep_file.new ./dep_file.orig

git log "$start_commit"..develop --reverse --pretty=format:"%H %s" | while IFS= read -r line; do
    # Process each line
    read -r commit_hash commit_message <<< "$line"
    echo "Checking out $commit_hash"
    git checkout "$commit_hash" >/dev/null 2>&1

    ./tools/modularity_check/modularity_check.py generate_dependency_file
    diff ./dep_file.orig ./tools/modularity_check/dep_file.new > /dev/null 2>&1 || {
        echo "    dep_file has changed as of commit $commit_hash !"
        echo "    $commit_message"
        echo " > for new lines, < for removed lines"
        diff ./dep_file.orig ./tools/modularity_check/dep_file.new
        echo "===================="
        cp ./tools/modularity_check/dep_file.new ./dep_file.orig
    }

done

# git checkout develop
