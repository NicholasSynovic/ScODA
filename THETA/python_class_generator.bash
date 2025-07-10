#!/bin/bash

# This script reads a CSV file and generates a Python SQLAlchemy-like Table definition.
# It assumes the first column is 'id' (Integer, primary_key=True) and others are Float.

# Check if a CSV file path is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <path_to_csv_file>"
    echo "Example: $0 data.csv"
    exit 1
fi

CSV_FILE="$1"

# Check if the CSV file exists
if [ ! -f "$CSV_FILE" ]; then
    echo "Error: CSV file '$CSV_FILE' not found."
    exit 1
fi

# Extract the filename without extension to use as the table name
FILE_NAME=$(basename "$CSV_FILE" .csv)

# Read the header row from the CSV file
HEADER=$(head -n 1 "$CSV_FILE")

# Initialize an array to hold column definitions
COL_DEFINITIONS=()

# Process the header to create column definitions
IFS=',' read -ra COLUMNS <<< "$HEADER"
first_column_processed=false

for col_name in "${COLUMNS[@]}"; do
    # Trim whitespace from column names
    col_name=$(echo "$col_name" | xargs)

    if [ "$first_column_processed" = false ]; then
        # The first column is always 'id' with Integer and primary_key=True
        COL_DEFINITIONS+=("Column(\"id\", Integer, primary_key=True)")
        first_column_processed=true
    else
        # All other columns are treated as Float
        COL_DEFINITIONS+=("Column(\"${col_name}\", Float)")
    fi
done

# Join the column definitions with a comma and newline, properly indented
# The 'printf' command is used to format each element before joining
JOINED_COLS=$(printf "            %s,\n" "${COL_DEFINITIONS[@]}")

# Remove the trailing comma and newline from the last column definition
# This ensures the last column doesn't have an unnecessary comma
JOINED_COLS=${JOINED_COLS%,*}

# Construct the final Python class string
PYTHON_CLASS_STRING=" _: Table = Table(\n"
PYTHON_CLASS_STRING+="            \"${FILE_NAME}\",\n"
PYTHON_CLASS_STRING+="            self.metadata,\n"
PYTHON_CLASS_STRING+="${JOINED_COLS}\n"
PYTHON_CLASS_STRING+="        )"

# Output the generated Python string
echo -e "$PYTHON_CLASS_STRING"
