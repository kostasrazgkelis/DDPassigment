#!/bin/bash

# Specify the number of iterations
iterations=10

# Specify the datasets
datasets=("dataset1K" "dataset100K" "dataset250K" "dataset500K" "dataset1000K")

# Specify the join methods
join_methods=("hash_join" "semi_join" "bloom_join")

# Output file name
output_file="execution_times.txt"

# Clear the output file if it exists
> "$output_file"

# Run the script multiple times for each dataset and join method
for join_method in "${join_methods[@]}"
do
    for dataset in "${datasets[@]}"
    do
        echo "Dataset: $dataset, Join Method: $join_method" >> "$output_file"
        total_time=0
        for ((i=1; i<=$iterations; i++))
        do
            echo "Iteration $i:" >> "$output_file"
            execution_time=$( { time python join_methods.py "$dataset" "$join_method"; } 2>&1 | grep real | awk '{print $2}' | sed 's/s$//' )
            total_time=$(awk "BEGIN{print $total_time + $execution_time}")
            echo "$execution_time" >> "$output_file"
            echo "----------------------------------------" >> "$output_file"
            echo "Pipeline finished for dataset: $dataset, Iteration $i:, join method: $join_method in $execution_time"
        done
        average_time=$(awk "BEGIN{printf \"%.2f\", $total_time / $iterations}")
        echo "Average Time: $average_time seconds" >> "$output_file"
        echo "" >> "$output_file"
    done
done
