#!/bin/bash

python dfs.py put_text pagecounts-20130601-000000 pagecounts-20130601-000000 3
python dfs.py put_class testmr/WikiMediaFilter\$FilterMapper.class testmr/WikiMediaFilter\$FilterMapper.class 3
python dfs.py put_class testmr/WikiMediaFilter\$FilterReducer.class testmr/WikiMediaFilter\$FilterReducer.class 3
python dfs.py put_class testmr/WikiMediaFilter.class testmr/WikiMediaFilter.class 3
java testmr.WikiMediaFilter pagecounts-20130601-000000 PageCountsOutput $1 $2
