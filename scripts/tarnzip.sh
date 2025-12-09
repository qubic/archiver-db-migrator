#!/bin/bash
for dir in ./*/     
do
    dir=${dir%*/}      # remove the trailing "/"
    echo "creating tar for directory ${dir##*/}..."    # print everything after the final "/"
    /usr/bin/tar -cf ${dir##*/}.tar ${dir}
    echo "zipping..."
    /usr/local/bin/bzip2 -9 ${dir##*/}.tar
    echo `ls ${dir##*/}.tar*`
done

