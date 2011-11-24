#!/bin/sh
#
# Rougly calculates the size of the (non-whitespace, non-comment) code
# This is a convenience script to make sure our "lightweight" statement on the
# project home page will still hold ;)
#
# Copyright (c) 2011 Vincent Driessen, @nvie
#

find_source_files() {
	find . -name '*.py' | egrep -v '(dummy|examples|setup|tests)'
}

dump_source_files() {
	find_source_files | xargs cat
}

filter_out_comments_and_whitespace() {
	grep -v '^\s*#' | grep -v '^\s*$' | grep -v '"""'
}

code_size() {
	dump_source_files | filter_out_comments_and_whitespace | wc -c
}

code_locs() {
	dump_source_files | filter_out_comments_and_whitespace | wc -l
}

echo "Size: $(code_size) kB"
echo "Lines: $(code_locs) kB"
