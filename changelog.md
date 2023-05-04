# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

### [0.0.18] - 2023-05-03
 - Added support for new conditions:
	- Cryptosporidiosis
	- Cyclosporiasis
	- Cholera and Vibriosis
	- Campylobacteriosis
	- Salmonellosis
	- STEC
	- Shigellosis
	- S Typhi and S Paratyphi
	- FoodNet
	- Candida Auris
	- CP-CRE
 - Updated Receiver to be able to handle files in sub folders.
 - Updated MMG-Based and MMG-SQL to support new feature of the new conditions
	- Elements mapped more than in one block
	- Handling properly complex fields that are not populated
	- Added check for empty single collections.
 - Create sub tables for repeating elements in repeating groups for MMG-in-SQL
 - Added unit test and code coverage to Receiver/Debatcher and Redactor functions.
 - Added support for file upload to cloud-transport service and support for up to 10Mb files.
 - Renamed MSH-21 column names to be one based instead of zero based.
 - Removed rule that validates OBR-22 against OBR-7
 - Added validation for MMWR week.
 - Updated PHIN Spec profiles to match discrepancies finding testing against MVPS
 - Fixed fortify scan Critical and High issues.
 - Updated Readme files with latest changes and features
 - Updated all process names and process status on all functions to be consistent.


### [0.0.11] -2023-04-19
  DEX Release 0.0.17
  
  - Added EventHub Metadata to Process schema.
  - Created dedicated TST database and populating it with gold tables.
  - Created Job for Lake of Segments Raw->Bronze->Silver
  - Bug fixes
     -  ELR messages were not being populated in gold.
     	

### [0.0.10] - 2023-04-06
   DEX Release 0.0.16
   
   - Separeted Database for Gold tables in DEV and TST
   - Parameterized workflow jobs in DEV and TST
   - Deployed DBX pipeline in STG.
   	- (waiting for terraforming workflows before deploying to STG)
   - Bug Fixes
   	- Redaction pipeline was not flowing to silver/gold tables.
   	- Fixed file_uuid on schema
   		
   		
### [0.0.9] - 2023-03-22
   DEX Release 0.0.15
   
   - Shortened column and table names
   - Created  pipeline for failed messages
   - Defined process to migrate workflow jobs from one environment to the next.
   - parameterized workflow jobs.
   - Bug fixes

### [0.0.8] - 2023-03-08
   DEX Release 0.0.14
   
   - Set up TST environment
   - Fixed issues to route messages with no Route properly
   - Created Raw, Bronze, silver and gold tables for Redactor
   - Updted Key Valut, scopes and keys per environment
   - Created widget for DBX_SCOPE
   - Created pipeline to generate list of all failed messages.

### [0.0.7] - 2023-02-22
  DEX Release 0.0.13
- Updated Structure Validation tables to follow same schema for process columns
- Created notebooks for Lake Of Segments Raw, Bronze and Silver tables.
- Added Classification column to structure validation tables.
- Onboarded CELR team
- Setting up TEST environment and pipeline
  

### [0.0.6] - 2023-02-08
  DEX Release 0.0.12
	
  - Updated Structure Validation and MMG Validaton report tables
  - Created merged Silver tables of OK and ERR for Structure and MMG Validation Reports
  - Recoded scala notebooks in python
  - Added Parameters to be able to run notebooks on different environments saving to environment specific database and storage accounts. Refactored all notebooks to use parameters.
  - Updated Config to pull event hub name and respective secret from corresponding Key Vault.
  - Created AD groups to allow customers access to DEX data.
  - Created database for Customers to start interacting with DEX and created Gold tables/views from develop database.
  - Created Workflow jobs to run data pipelines.


### [0.0.5] - 2023-01-25
  - DEX Release 0.0.11
  
  - Creted common schema for all Bronze Tables with common function for table creation.
  - Created Notebooks to generate bronze table from all Event Hub Raw tables:
    - hl7-recdeb-ok, hl7-recdeb-err 
    - hl7-structure-ok, hl7-structure-err
    - hl7-mmg-validation-ok, hl7-mmg-validation-err
    - hl7-mmg-based-ok, hl7-mmg-based-err
    - hl7-mmg-sql-ok, hl7-mmg-sql-err
  - Created Notebooks to generate Silver and Gold table for Validation tables:
    - hl7-structure-ok, hl7-structure-err
    - hl7-mmg-validation-ok, hl7-mmg-validation-err
  - Created Notebooks to generate Silver and Gold tables of data product tables:
    - hl7-mmg-based-ok
    - hl7-mmg-sql-ok
   
  
### [0.0.4] - 2023-01-11
  - DEX Release 0.0.10
  
  - Refactor dbx notebook repo Structure
  - Include errorCount and WarningCount columns on validation bronze tables. 
  - Create Silver Tables
  - hl7_structure_err_bronze

### [0.0.3] - 2022-12-14
  - DEX Release 0.0.9

  - Created Databricks bronze tables from Event Hubs Delta table for after applying MMG Model for Structure Validation.
  - Flattened Structure Error table to list repeating blocks
  - Modified Bronze tables to read as stream on top of Delta tables for live update.
  - Created view in Databricks combing Structure Error and Structure Ok messages into a single view v_hl7_structure_bronze_stream
  - Created Databricks Dashboard/Visuals for Structure Validation

### [0.0.2] - 2022-11-29
  - DEX Release 0.0.8
  - Created Notebooks to Read data from 2 new Event Hubs and create Delta tables
    - eh-hl7-message-processor-err
    - eh-hl7-message-processor-ok
  -  Created Notebook to read data from message-processor Delta Table and create a structured MMG based Model


### [0.0.1] - 2022-11-16
  - DEX Release 0.0.7

  - Created Notebooks to Read data from Event Hubs and create Delta tables
      - eh_hl7_file_dropped
      - eh-hl7-recdeb-ok
      - eh-hl7-recdeb-err
      - eh-hl7-mmg-validation-ok
      - eh-hl7-mmg-validation-err
      - eh-hl7-structure-ok
      - eh-hl7-structure-err
