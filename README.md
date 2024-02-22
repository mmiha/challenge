# Challenge

This project sets up a Databricks instance and schedules a worflow to run
every day that processes event logs of advertisements and related user tracking 
events to find aggregate quantities based on this data.

## Set up

```
cd infrastructure/
terraform init
terraform apply
```