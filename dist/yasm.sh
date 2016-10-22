#! /bin/sh

# Libtool support for yasm files, expect the first argument to be a path to
# the source file and the second argument to be a path to libtool's .lo file.
# Use the second argument plus libtool's -o argument to set the real target
# file name.
source=$1
target=`dirname $2`
while test $# -gt 0
do
	case $1 in
	-o)
		target="$target/$2"
		shift; shift;;
	*)
		shift;;
	esac
done

yasm -f x64 -f elf64 -X gnu -g dwarf2 -D LINUX -o $target $source
