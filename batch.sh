#!/bin/bash

# 循环执行命令
#for i in {0..50}; do
#    # 执行命令并将输出重定向到对应的文件中
#    make project3 > "$i.out" 2>&1
#done
#for i in {0..50}; do
#    echo "=-------------- $i -----------------" >> outputs.out
#    # 使用正确的输出文件名进行grep搜索
#    grep "PASS" "$i.out" | sed -E 's/(PASS: )([^ ]+)( .*)?/\2/' >> "$i.diff"
#    diff "$i.diff" successful_tests.out >> outputs.out
#    rm "$i.diff"
#done
#
rm *.out
echo $LOG_LEVEL
for i in {0..99}; do
    echo "---------------------$i-------------------" >> result.out
    make project3 >> "$i.out"
    grep "FAIL" "$i.out"  | wc -l >> result.out
    grep -C 50 "FAIL" "$i.out" >> "result.out"
done

