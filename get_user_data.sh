#!/usr/bin/env bash

sacct -P -n -a --format JobID,User,Group,State,Cluster,AllocCPUS,REQMEM,TotalCPU,Elapsed,MaxRSS,ExitCode,NNodes,NTasks $@
