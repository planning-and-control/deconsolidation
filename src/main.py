import pandas as pd
from variables import dtypes
from functions import *

#Set paths
path_DataReport_Conso_BP19_22R_ex_F00 = r"C:\Users\E353952\Desktop\code-projects\consolidation-script\output\BP2025_exF00.csv"
path_FC19_for_F00_BP = r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\F00\BU21\F00_FC20_BS.csv"
path_dim_company = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Bypass_BU21.xlsx"
path_dimgrowth = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Base_Growth\Budget\2021\Company_Classification.xlsx"
path_levels = r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\Export\DataReport - Unprocessed\Levels\Levels_Report_0.xlsx"
path_extramappings = r"C:\Users\E353952\EDP\O365_P&C Corporate - Documents\General\OtherSources\Extra-Mappings.xlsx"
path_extramappings_bu21 = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Extra-Mappings_BU21.xlsx"
path_consoflag=r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\Export\Consolidation\Conso_Flag\Results\FullBackup\DataReport_SellDown_Flag_0.xlsx" 

#levels dataframe
df_dimlevels = pd.read_excel(path_levels, dtype=dtypes["levels"], skiprows=range(3), sheet_name="Accounts")
df_extramappings_bu21 = pd.read_excel(path_extramappings_bu21, dtype=dtypes["extramappingsbu21"], sheet_name="Dim_AdaptiveLevels")
df_dimlevels = transform_levels(df_dimlevels, df_extramappings_bu21)

#company, growth and partner dataframes
df_extramappings = pd.read_excel(path_extramappings, dtype=dtypes["extramappings"], sheet_name="Dim_Partner")
df_dim_company = pd.read_excel(path_dim_company, dtype=dtypes["dim_company"], sheet_name="Sheet6")
df_dimgrowth = pd.read_excel(path_dimgrowth, dtype=dtypes["company_classif"], sheet_name="SPV_classification")
df_dim_partner = transform_dimpartner(df_dim_company, df_extramappings, df_dimgrowth)

#datareport dataframe
df_DataReport = pd.read_csv(path_DataReport_Conso_BP19_22R_ex_F00, dtype=dtypes["DataReport"], parse_dates=["dataPeriod"])

#F00 from previous month
df_F00_prevmonth = transform_F00(df_DataReport)

#fc19 dataframe
df_FC19 = pd.read_csv(path_FC19_for_F00_BP, dtype=dtypes["FC19"], parse_dates=["dataPeriod"])
df_FC19 = transform_FC(df_FC19, df_dimlevels)

#consoflag
df_consoflag=pd.read_excel(path_consoflag, dtype=dtypes["SellDown_ConsoFlag"], parse_dates=["Period"])

#Transforming Data
df_deconsoliditation = pd.concat([df_DataReport, df_FC19, df_F00_prevmonth], ignore_index=True)
print(df_deconsoliditation.shape)

check_list = [df_deconsoliditation, df_consoflag]
for dataframe in check_list:
    print(dataframe.isnull().sum())

df_deconsoliditation = transform_deconsolidation(df_deconsoliditation, df_consoflag)

print("Generating csv...")

df_DataReport.to_csv("../output/datareport.csv", index=False)
df_FC19.to_csv("../output/df_FC19.csv", index=False)
df_F00_prevmonth.to_csv("../output/df_F00_prevmonth.csv", index=False)
df_deconsoliditation.to_csv("../output/deconsolidation.csv", index=False)
