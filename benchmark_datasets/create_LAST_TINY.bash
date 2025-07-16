#!/bin/bash

# Find all CSV files in the ./LAST directory and its subdirectories
find ./LAST -type f -name "*.csv" | while read file; do
    # Extract the filename without the directory path
    filename=$(basename "$file")

    # Extract the directory path relative to ./LAST
    relative_dir=$(dirname "$file" | sed 's|./LAST||')

    # Create the corresponding directory structure in the current directory
    mkdir -p "./LAST_TINY/$relative_dir"

    # Use cat and head to append the first 101 lines to the file in the corresponding directory
    cat "$file" | head -n 101 > "./LAST_TINY/$relative_dir/$filename"
done
