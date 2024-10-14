# Databricks notebook source
storage_account_name = "freefinalstorageaccount"

sas_token = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-11-14T14:04:23Z&st=2024-10-14T06:04:23Z&spr=https&sig=QrQwN1nscBt3xLJb%2F%2B9tN7zEGhgnKseLl4XkNLrBUdQ%3D"

# Set Spark configuration for accessing the storage account using SAS token
spark.conf.set(f"fs.azure.sas.output.{storage_account_name}.dfs.core.windows.net", sas_token)

