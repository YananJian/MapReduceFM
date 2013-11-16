#!/bin/bash

python dfs.py put_text WordCount.in WordCountInput 3
python dfs.py put_class testmr/WordCount.class testmr/WordCount.class 3
python dfs.py put_class testmr/WordCount\$WordCountMapper.class testmr/WordCount\$WordCountMapper.class 3
python dfs.py put_class testmr/WordCount\$WordCountReducer.class testmr/WordCount\$WordCountReducer.class 3
java testmr/WordCount WordCountInput WordCountOutput $1 $2
