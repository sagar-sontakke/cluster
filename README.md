---------------------
Contents
---------------------

1. Introduction
2. How to use it?
3. How to test it?
4. Working mechanism
5. References

---------------------


1. Introduction

	- This is cluster-programming library that provides the interfaces for creating/using
	  the clusters containing multiple server nodes
	- The working is simple and easy to use the abstraction functions
	- The cluster configuration is to be specified in a configuration file
	- The main function of these library are described below


2. How to use it?

	- Specifying the cluster configuraton with config file:
		- This file contains the details about each server in the cluster
		- The name of this file is as specified by the user
		- This file contains the entries as a pair of server id and IP:port address which
		  are comma separated eg:  

			    200, tcp://127.0.0.1:1201  
			    300, tcp://127.0.0.1:1202  

		- The node ids and ip:port addresses for all the servers in a cluster must be unique
		  else there will be a parse error

	- This library provides a number of basic functions to use for cluster set-up and managing
	  communications between them. These function roles are explained in the corresponding comments
	  in the code


3. How to test it?

	- Go to the directory where the cluster_test.go file is present
	- Set the testing parameters (default values are also set) like when to timeout, how many times 
	  to loop, whether to broadcast/unicast messages and display intervals (after how much time to 
	  show the current status)
	- The functions of these parameters are mentioned in the comments
	- To run, hit the following command:
		$ go test github.com/sagar-sontakke/

4. References

	- Go language tutorial (http://tour.golang.org/#1)
	- Go package info (for atomic instructions) (http://golang.org/pkg/sync/atomic/#AddInt64)
