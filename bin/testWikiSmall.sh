#!/bin/bash

python dfs.py put_text pagecounts_small.in pagecounts_small_input 3
python dfs.py put_class testmr/WikiMediaFilter\$FilterMapper.class testmr/WikiMediaFilter\$FilterMapper.class 3
python dfs.py put_class testmr/WikiMediaFilter\$FilterReducer.class testmr/WikiMediaFilter\$FilterReducer.class 3
python dfs.py put_class testmr/WikiMediaFilter.class testmr/WikiMediaFilter.class 3
java testmr.WikiMediaFilter pagecounts_small_input pagecounts_small_output $1 $2
