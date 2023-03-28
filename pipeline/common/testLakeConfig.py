# COMMAND  --------------

lakeConfig = LakeConfig(database, database_folder)
lakeDAO = LakeDAO(lakeConfig)
 
topic = "hl7_structure_ok"
rawDF = lakeDAO.readStreamFrom(f"{topic}_eh_raw")
                                
display(rawDF)