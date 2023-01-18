# Databricks notebook source
class LakeConfig:
    def __init__(self, rootFolder, dbName):
        self.rootFolder = rootFolder
        self.dbName = dbName
        self.tableName = tableName
        
    def getSchemaName(self, tableName):
        return self.dbName + "." + tableName
    
    def getCheckpointLocation(self, tableName):
        return self.rootFolder + "events/" + tableName + "/_checkpoint"

# COMMAND ----------

def normalizeString(str):
    return str.replace("-", "_").lower()
