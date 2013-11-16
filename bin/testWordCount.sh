#!/bin/bash

python dfs.py put_text input wordCountInput 3
python dfs.py put_class testmr/WordCount.class testmr/WordCount.class 3
python dfs.py put_class testmr/WordCount\$WordCountMapper.class testmr/WordCount\$WordCountMapper.class 3
python dfs.py put_class testmr/WordCount\$WordCountReducer.class testmr/WordCount\$WordCountReducer.class 3
java testmr/WordCount wordCountInput wordCountOutput $1 $2
