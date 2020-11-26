
# Features

## W 的算法： 
unack表示被worker取走但未完成的数目。 backlog - unack  表示待处理的消息。但是当前无法知道有多少worker正在处理unack，也就没法算出需要增加多少台机器。

## HPA的Algorithm
看是否能改动HPA的ALgorithm。

## Job lifecycle & Worker Efficency
增加Job lifecycle。 根据lifecycle算出Worker的效率


# Bug Fix

## external-server算出的W 和 HPA的 Replica 不是完全同步
external-server已经给出W==1，但是HPA的replica没有及时变成1
