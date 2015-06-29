#!/bin/bash

echo "cleaning old files..."

rm -r /home/guest/cdse/accumulo/hadoop-1.0.4/hdfs_data_dir/*
rm -r /home/guest/cdse/accumulo/hadoop-1.0.4/hdfs_name_dir/*
rm -r /home/guest/cdse/accumulo/zookeeper-3.3.6/zoo_data_dir/*

echo "setting proper permissions..."
chmod g-w /home/guest/cdse/accumulo/hadoop-1.0.4/hdfs_data_dir
chmod g-w /home/guest/cdse/accumulo/hadoop-1.0.4/hdfs_name_dir

echo "updating environment..."
cat "export ACCUMULO_HOME=/home/guest/cdse/accumulo/accumulo-1.4.2" >> ~/.bashrc
sudo sysctl vm.swappiness=5

echo "initializing HDFS and starting utils..."
/home/guest/cdse/accumulo/hadoop-1.0.4/bin/hadoop namenode -format <<< $'Y\n'
zkServer.sh start
/home/guest/cdse/accumulo/hadoop-1.0.4/bin/start-all.sh

sleep 5

echo "initializing and starting Accumulo"
/home/guest/cdse/accumulo/accumulo-1.4.2/bin/accumulo init <<< $'acc\nacc\nacc\n'
/home/guest/cdse/accumulo/accumulo-1.4.2/bin/start-all.sh
