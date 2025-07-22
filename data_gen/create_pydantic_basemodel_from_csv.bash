#!/bin/bash

# Check if a CSV file is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <csv_file>"
    exit 1
fi

CSV_FILE="$1"

# Extract the header row from the CSV file
HEADER=$(head -n 1 "$CSV_FILE")

# Replace spaces with underscores and remove quotes
FIELDS=$(echo "$HEADER" | sed 's/ /_/g' | sed 's/"//g')

# Generate the Pydantic BaseModel
MODEL_NAME="GeneratedModel"
echo "from pydantic import BaseModel"
echo ""
echo "class $MODEL_NAME(BaseModel):"

# Loop through the fields and generate the model attributes
IFS=',' # Set the delimiter to comma
for FIELD in $FIELDS; do
    echo "    $FIELD: int"
done
