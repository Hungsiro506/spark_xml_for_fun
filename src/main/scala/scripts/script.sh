#!/bin/bash

for file in *.XML
do
        sed -i '1,2d' "$file"
done

