#!/bin/bash

listing_counter=0
review_counter=0
for filename in **/**; do
    if [[ $filename == *"review"* ]]; then
        echo "processing review for $filename"
        if [[ $review_counter -ge 1 ]]; then
            awk 'NR>1 { print $0 }' $filename >> master_reviews.csv
        else
            echo "Using file to create initial reviews"
            awk '{ print $0 }' $filename > master_reviews.csv
        fi
        ((review_counter=review_counter+1))
    elif [[ $filename == *"listing"* ]]; then
        echo "processing listing for $filename"
        if [[ $listing_counter -ge 1 ]]; then
            awk 'NR>1 { print $0 }' $filename >> master_listings.csv
        else
            echo "Using file to create initial listings"
            awk '{ print $0 }' $filename > master_listings.csv
        fi
        ((listing_counter=listing_counter+1))
    fi
done
