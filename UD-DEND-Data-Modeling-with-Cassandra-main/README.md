# Udacity Data Engineering Nanodegree - Project 2/6
[![Made withJupyter](https://img.shields.io/badge/Made%20with-Jupyter-orange?style=flat-square&logo=Jupyter)](https://jupyter.org/try)
[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg?style=flat-square&logo=Python)](https://www.python.org/)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg?style=flat-square&logo=Microsoft-Academic)](https://lbesson.mit-license.org/)


## Data Modeling with Cassandra 
---  
  
## Introduction 
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

So we need a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions.


## Project Description

The following applies in this project:
- [x] Modeling an Apache Cassandra database
- [x] Create tables that are aligned to the queries
- [x] Insert the relevant data to the table
- [x] Check if the Selects are correct

## Requirements

This project was done on a Linux-OS ([Ubuntu 20.04 LTS](https://ubuntu.com/download/desktop)) with the source-code editor [Visual Studio Code](https://code.visualstudio.com/).

To implement the project you will need the following things:
- Python
- Apache Cassandra
  - [Here you can find an tutorial to install Apache Cassandra on Ubuntu](https://www.tecmint.com/install-apache-cassandra-on-ubuntu/)
- Jupyter

To work with Apache Cassandra and Python, you have to install the following module:
```bash
  pip install cassandra-driver
```
## Project Datasets

For this project, we'll be working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
