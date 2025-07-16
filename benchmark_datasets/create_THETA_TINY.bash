#!/bin/bash

# Find all CSV files in the ./THETA directory and its subdirectories
find ./THETA -type f -name "*.csv" | while read file; do
    # Extract the filename without the directory path
    filename=$(basename "$file")

    # Extract the directory path relative to ./THETA
    relative_dir=$(dirname "$file" | sed 's|./THETA||')

    # Create the corresponding directory structure in the current directory
    mkdir -p "./THETA_TINY/$relative_dir"

    # Use cat and head to append the first 101 lines to the file in the corresponding directory
    cat "$file" | head -n 101 > "./THETA_TINY/$relative_dir/$filename"
done
