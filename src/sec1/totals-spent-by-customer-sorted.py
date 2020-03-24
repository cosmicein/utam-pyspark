#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 21 13:46:22 2020

@author: gosundar
"""
from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

conf = SparkConf().setMaster("local").setAppName("TotalSpendingSorted")
sc = SparkContext(conf = conf)

input = sc.textFile("/Users/archanababu/dev/repo/spark/udemy-taming-big-data/data/customer-orders.csv")
mappedInput = input.map(parseLine)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
totalByCustomerSorted = flipped.sortByKey()

results = totalByCustomerSorted.collect();
for t in results:
    print(t)