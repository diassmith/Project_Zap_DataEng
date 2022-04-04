# Databricks notebook source
df1 = spark.read.format("json").load("dbfs:/mnt//bronze/source-4-ds-train.json")
