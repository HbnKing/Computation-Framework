#!/bin/bash

# filename: wordcount.sh
# usage: word count

# handle position arguments
if [ $# -ne 1 ]
then
    echo "Usage: $0 filename"
    exit -1
fi

# realize word count
printf "%-14s%s\n" "Word" "Count"

cat $1 | tr 'A-Z' 'a-z' | \
egrep -o "\b[[:alpha:]]+\b" | \
awk '{ count[$0]++ }
END{
for(ind in count)
{ printf("%-14s%d\n",ind,count[ind]); }
}' | sort -k2 -n -r | head -n 10


#Use the tr command to convert all uppercase letters to lowercase letters,
# then use the egrep command to grab all the words in the text and output them item by item.
# Finally, use the awk command and the associative array to implement the word count function,
# and decrement the output according to the number of occurrences.
#