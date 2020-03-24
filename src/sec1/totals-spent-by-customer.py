#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 21 12:49:27 2020

@author: gosundar
"""

from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    amount = float(fields[2])
    return (customer, amount)

conf = SparkConf().setMaster("local").setAppName("TotalSpending")
sc = SparkContext(conf = conf)

lines  = sc.textFile("/Users/archanababu/dev/repo/spark/udemy-taming-big-data/data/customer-orders.csv")
rdd = lines.map(parseLine)
total = rdd.reduceByKey(lambda x, y: x + y)

results = total.collect();
for result in results:
    print(result)
