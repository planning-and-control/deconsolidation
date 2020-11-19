import pandas as pd
from variables import dtypes, adaptive_version
from functions import *

#Set paths
path_DataReport_Conso_BP19_22R_ex_F00 = r"C:\Users\E353952\Desktop\code-projects\consolidation-script\output\BP2025_exF00.csv"
path_0LIA01=r"C:\Users\E353952\Desktop\code-projects\consolidation-script\output\BP2025_0LIA01.csv"
path_IFRS000=r"C:\Users\E353952\Desktop\code-projects\consolidation-script\output\BP2025_1IFRS000.csv"
path_FC19_for_F00_BP = r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\F00\BU21\F00_FC20_BS.csv"
path_dim_company = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Bypass_BU21.xlsx"
path_dimgrowth = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Base_Growth\Budget\2021\Company_Classification.xlsx"
path_levels = r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\Export\DataReport - Unprocessed\Levels\Levels_Report_0.xlsx"
path_extramappings = r"C:\Users\E353952\EDP\O365_P&C Corporate - Documents\General\OtherSources\Extra-Mappings.xlsx"
path_extramappings_bu21 = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Extra-Mappings_BU21.xlsx"
path_consoflag=r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\Export\Consolidation\Conso_Flag\Results\FullBackup\DataReport_SellDown_Flag_0.xlsx" 
path_FC19_for_F00_PL=r"C:\Users\E353952\EDP\O365_Adaptive - Documents\General\F00\BU21\F00_FC20_PL.csv"
path_magnitude_SIM = r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\Accounts\Sources\2020\09\Mapeamento Conta Operacional SIM-F - Magnitude_LOI.xlsx"
path_cecosmap=r"C:\Users\E353952\EDP\O365_P&C Data Lake - General\MetaDataSources\CostCenter\Budget\2021\Dim_CostCenter_2021B.xlsx"


#levels dataframe
df_dimlevels = pd.read_excel(path_levels, dtype=dtypes["levels"], skiprows=range(3), sheet_name="Accounts")
df_extramappings_bu21 = pd.read_excel(path_extramappings_bu21, dtype=dtypes["extramappingsbu21"], sheet_name="Dim_AdaptiveLevels")
df_dimlevels = transform_levels(df_dimlevels, df_extramappings_bu21)

df_dimlevels_copy = df_dimlevels.copy()
df_dimlevels_pl = transform_dimlevels_pl(df_dimlevels_copy)
print("Dimlevels rows: ", df_dimlevels.shape[0])
print("Dimlevels PL rows: ", df_dimlevels_pl.shape[0])

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
df_dimlevels_copy = df_dimlevels.copy()
df_FC19 = pd.read_csv(path_FC19_for_F00_BP, dtype=dtypes["FC19"], parse_dates=["dataPeriod"])
df_FC19 = transform_FC(df_FC19, df_dimlevels_copy)

#consoflag
df_consoflag=pd.read_excel(path_consoflag, dtype=dtypes["SellDown_ConsoFlag"], parse_dates=["Period"])
# df_consoflag_filtered = df_consoflag

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

#FC20 P&L
df_fc20_pl = pd.read_csv(path_FC19_for_F00_PL, dtype=dtypes["F00_PL"], parse_dates=["dataPeriod"])
df_fc20_pl = transform_fc20_pl(df_fc20_pl, df_dimlevels_pl)


df_datareport_conso = pd.concat([df_DataReport, df_FC19, df_fc20_pl, df_F00_prevmonth, df_deconsoliditation]).reset_index(drop=True)

for i, dataframe in enumerate([df_DataReport, df_FC19, df_fc20_pl, df_F00_prevmonth, df_deconsoliditation]):
    print(i, " number of rows: ", dataframe.shape[0])

df_datareport_conso.D_SC = df_datareport_conso.D_SC.str.replace("FC20 (3+9)", adaptive_version, regex=False)
print(df_datareport_conso.D_SC.unique())
filter_1 = df_datareport_conso.dataPeriod.dt.year > 2021
index_drop = df_datareport_conso[filter_1].index
df_datareport_conso.drop(index_drop, inplace=True)
df_datareport_conso.reset_index(drop=True, inplace=True)
df_datareport_conso.to_csv("../output/datareport_conso.csv", index=False)
df_dimlevels.to_csv("../output/df_dimlevels.csv", index=False)

#bu_emp_load
df_0LIA01 = pd.read_csv(path_0LIA01, parse_dates=["dataPeriod"], dtype=dtypes["0LIA01"])
df_IFRS000 = pd.read_csv(path_IFRS000, parse_dates=["dataPeriod"], dtype=dtypes["0LIA01"])
df_cecosmap=pd.read_excel(path_cecosmap, sheet_name="Dim_CoCe", dtype=dtypes["cecosmap"])
df_cecosmap.to_csv("../output/cecosmap.csv", index=False)
df_cecoslist = transform_0LIA(df_0LIA01, df_IFRS000, df_dimlevels, df_cecosmap)
df_fx = pd.read_excel(path_dim_company, sheet_name="Periods_FX", parse_dates=["Date"], dtype=dtypes["fx"])
df_fx = transform_fx(df_fx)
df_sim = transform_mappingsim(path_magnitude_SIM)
df_load = transform_load(df_datareport_conso, df_dimlevels, df_sim, df_fx, df_cecoslist)
df_load.to_csv("../output/BU21_EMP_Load.csv", index=False)