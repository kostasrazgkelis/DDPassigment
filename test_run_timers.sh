#!/bin/bash

# Specify the number of iterations
iterations=10

# Specify the dataset and join method
dataset="dataset100k"
join_method="hash_join"

# Run the script multiple times
for ((i=1; i<=$iterations; i++))
do
    echo "Iteration $i:"
    time python join_methods.py "$dataset" "$join_method"
    echo "----------------------------------------"
done

