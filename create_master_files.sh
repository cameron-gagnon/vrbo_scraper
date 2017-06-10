#!/bin/bash

for filename in **/**; do
    if [[ $filename == *"review"* ]]; then
        echo "processing review for $filename"
        awk 'NR>1 { print $0 }' $filename >> master_reviews.csv
    elif [[ $filename == *"listing"* ]]; then
        echo "processing listing for $filename"
        awk 'NR>1 { print $0 }' $filename >> master_listings.csv
    fi
done
