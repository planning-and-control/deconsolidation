import pandas as pd
from itertools import repeat
import numpy as np
from variables import adaptive_version, dtypes
import xlrd
import datetime
import re

def read_path(input_all_paths, denomination):
    df = pd.read_excel(input_all_paths, sheet_name="inputs")
    return df[df.denomination==denomination].path.iloc[0]

#------------REVIEW FILTER------------
def add_year(df, year):
    df_2 = df.copy()
    filter_1 = df_2.dataPeriod.dt.year < year
    filter_2 = df_2.dataPeriod.dt.month == 1
    index_drop = df_2[~filter_1 | ~filter_2].index
    df_2.drop(index_drop, inplace=True)
    df_2.reset_index(drop=True, inplace=True)
    df_2.rename(columns={"dataPeriod": "dataPeriod_prev"}, inplace=True)
    df_2["dataPeriod"] = df_2["dataPeriod_prev"] + pd.offsets.DateOffset(years=1)
    df_2.drop("dataPeriod_prev", axis=1, inplace=True)
    return df_2

#------------REVIEW FILTER------------
def transform_F00(df):
    #filter 2025
    filter_nulls = df.FlowAccount.isnull()
    filter_2025 = df.dataPeriod.dt.year == 2025
    index_drop = df[filter_nulls | filter_2025].index

    df = df.drop(index_drop).reset_index(drop=True)

    #Rename column
    df.rename(columns={"dataPeriod": "dataPeriod_prev"}, inplace=True)

    #Drop columns
    col_drop = ["FlowAccount", "AccountName", "AccountCode", "intercoAccount"]
    df.drop(col_drop, axis=1, inplace=True)

    #Add columns
    df["intercoAccount"] = "C"
    df["dataPeriod"] = df["dataPeriod_prev"] + pd.offsets.YearBegin()
    df["FlowAccount"] = "F00"
    df.loc[:,"AccountName"] = df.codeAcc+ "_F00_CH"
    df.loc[:,"AccountCode"] = df.codeAcc+ "_F00_CH"

    df.drop("dataPeriod_prev", axis=1, inplace=True)

    years = range(2022, 2026)
    list_df = map(add_year, repeat(df), years)
    list_df = [*list_df]
    list_df.append(df)
    df_concat = pd.concat(list_df).reset_index(drop=True)
    
    col_drop = ["Scope", "Scope_T1"]
    df_concat.drop(col_drop, axis=1, inplace=True)
    group_cols = [col for col in df_concat.columns if col not in ["LC_Amount"]]
    df_concat = df_concat.groupby(group_cols, as_index=False).sum()

    filter_1 = df_concat.dataPeriod.dt.year > 2021
    index_drop = df_concat[filter_1].index
    df_concat = df_concat.drop(index_drop).reset_index(drop=True)

    return df_concat

def transform_0LIA(df_0LIA01, df_IFRS000, df_dimlevels, df_cecosmap):
    df = pd.concat([df_0LIA01, df_IFRS000], ignore_index=True)

    keep_col = ["LevelName", "CostCentre"]
    drop_col = [col for col in df.columns if col not in keep_col]
    df.drop(drop_col, inplace=True, axis=1)

    df.drop_duplicates(subset=keep_col, inplace=True, ignore_index=True)

    df = pd.merge(df, df_dimlevels[["Lavel Name", "Company"]], left_on="LevelName", right_on="Lavel Name", how="left")
    df.drop("Lavel Name", axis=1, inplace=True)
    df.rename(columns={"Company": "D_RU"}, inplace=True)

    df["Is Number"] = np.where(df["CostCentre"].str.fullmatch(r"\d+"), df["CostCentre"], "Change")
    df["Length4or8"] = np.where((df["Is Number"] != "Change") & (df["Is Number"].str.len().isin([4,8])), df["D_RU"] + df["Is Number"].str[-4:], "Change")
    
    df = pd.merge(df, df_cecosmap[["Adaptive PL", "Profit Center"]], left_on="LevelName", right_on="Adaptive PL", how="left")
    df.drop("Adaptive PL", axis=1, inplace=True)
    df.rename(columns={"Profit Center": "Profit Center PL"}, inplace=True)

    df = pd.merge(df, df_cecosmap[["Adaptive BS", "Profit Center"]], left_on="LevelName", right_on="Adaptive BS", how="left")
    df.drop("Adaptive BS", axis=1, inplace=True)
    df.rename(columns={"Profit Center": "Profit Center BS"}, inplace=True)

    df.to_csv("borrar.csv", index=False)

    df["Profit Center"] = np.where(
        df["Length4or8"] != "Change",
        df["Length4or8"],
        np.where(
            df["Profit Center PL"].notnull(),
            df["Profit Center PL"],
            np.where(
                df["Profit Center BS"].notnull(),
                df["Profit Center BS"],
                df["D_RU"]+"0000"
                )
            )
        )
    df.to_csv("borrar2.csv", index=False)
    df["LevelCeCo"] = df["LevelName"] + "_" + df["CostCentre"]

    df.drop(["Is Number", "Length4or8", "Profit Center PL", "Profit Center BS"], axis=1, inplace=True)
    df.drop_duplicates(subset=["LevelName", "CostCentre", "D_RU"], inplace=True, keep="first", ignore_index=True)
    df.to_csv("borrar3.csv", index=False)
    return df



def transform_FC(df, df_dimlevels):
    #drop null rows in dimlevels
    nulls = df_dimlevels.Company.isnull()
    filter_1 = df_dimlevels["Level_type"] != "Company"
    index_drop = df_dimlevels[nulls | filter_1].index
    df_dimlevels.drop(index_drop, inplace=True)
    df_dimlevels.reset_index(drop=True, inplace=True)

    #merge to get level name
    df = df.merge(df_dimlevels[["Lavel Name", "Company"]], how="left", left_on="D_RU", right_on="Company")
    df.drop(["Company", "D_RU"], axis=1, inplace=True)
    df.rename(columns={"Lavel Name": "LevelName"}, inplace=True)

    #add Scenario
    df["D_SC"] = adaptive_version

    #add columns
    df["Period_Level"] = df.LevelName + "_" + df.dataPeriod.dt.strftime("%Y_%m")
    df["Partner_Level"] = df.Partner + "_" + df.dataPeriod.dt.strftime("%Y_%m")

    return df

#------------REVIEW FILTER------------
#no longer use - delete in future versions
def transform_FC19(df, df_dim_company, df_dim_partner, df_dimlevels):
    #drop null rows
    nulls = df[df.isnull()].index
    df.drop(nulls, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #merge with dimcompany
    df = df.merge(df_dim_company[["Company SIM", "Company SAP"]], how="left", left_on="D_RU", right_on="Company SIM")
    df.drop("Company SIM", axis=1, inplace=True)

    #merge with dimpartner
    # df = df.merge(df_dim_partner[["Partner SIM", "Partner SAP"]], how="left", left_on="T1", right_on="Partner SAP")
    # df.drop("Partner SIM", axis=1, inplace=True)

    #drop null rows in dimlevels
    nulls = df_dimlevels.isnull()
    filter_1 = df_dimlevels.Level_type == "Company"
    index_drop = df_dimlevels[nulls & ~filter_1].index
    df_dimlevels.drop(index_drop, inplace=True)
    df_dimlevels.reset_index(drop=True, inplace=True)
    
    #replace values
    df_dimlevels.Company = df_dimlevels.Company.replace({"U149": "U149-EM", "U271": "U271-EM"})

    #merge to get level name
    df = df.merge(df_dimlevels[["Lavel Name", "Company"]], how="left", left_on="Company SAP", right_on="Company")
    df.drop("Company", axis=1, inplace=True)
    df.rename(columns={"Lavel Name": "LevelNameSAP"}, inplace=True)

    df = df.merge(df_dimlevels[["Lavel Name", "Company"]], how="left", left_on="D_RU", right_on="Company")
    df.drop("Company", axis=1, inplace=True)
    df.rename(columns={"Lavel Name": "LevelNameSIM"}, inplace=True)

    #add columns
    df["LevelName"] = np.where(df.LevelNameSIM.isnull(), df.LevelNameSAP, df.LevelNameSIM)
    df["CostCentre"] = "uncategorized"
    df["codeAcc"] = df["D_AC"]
    df["Partner"] = np.where(df.PartnerSAP == "#", "Partner_CH", df.PartnerSAP)

    #drop columns
    drop_cols = [
        "Partner SAP",
        "Company SAP",
        "D_SC",
        "D_RU",
        "T1",
        "D_SP",
        "D_CO",
        "RU_Scope",
        "D_PE",
        "D_CU",
        "LevelNameSAP",
        "LevelNameSIM"
    ]

    df.drop(drop_cols, axis=1, inplace=True)

    #add Scenario
    df["D_SC"] = adaptive_version

    #rename columns
    df.rename(columns={"D_FL": "FlowAccount", "D_AC": "AccountCode"}, inplace=True)

    #recreate df for each year and add date column
    list_dfs = []
    for year in range(2020, 2026):
        year = str(year)
        df_append = df.copy()
        df_append["dataPeriod"] = pd.to_datetime(f"{year}-01-01")
        list_dfs.append(df_append)
    
    df = pd.concat(list_dfs, ignore_index=True)

    #add columns
    df["intercoAccount"] = "C"
    df["AccountName"] = df.codeAcc
    df["Period_Level"] = df.LevelName + "_" + df.dataPeriod.dt.strftime("%Y_%m")
    df["Partner_Level"] = df.Partner + "_" + df.dataPeriod.dt.strftime("%Y_%m")

    df.to_csv("prueba.csv")

    return df

#------------REVIEW FILTER------------
def transform_dimpartner(df, df_extramappings, df_dimgrowth):
    '''Returns Dim_Partner.
    '''
    #concat
    df = pd.concat([df, df_extramappings], ignore_index=True)

    #rename columns
    df.rename(columns={
        "Company SIM": "Partner SIM",
        "Company SAP": "Partner SAP",
        "Company Name SAP": "Partner Name SAP",
        "Platform": "Partner Platform",
        "Country": "Partner Country"
    }, inplace=True)

    #drop columns
    drop_cols = ["Consolidation Method", "% EBITDA CONTRIBUTION", "% NET OWNERSHIP", "Reporting Segment"]
    df.drop(drop_cols, axis=1, inplace=True)

    #filter rows
    filter_1 = df["Partner SAP"] == "U149"
    filter_2 = df["Partner SAP"] == "U271"
    filter_3 = df["Partner SAP"] == "U136"
    combined_filter = filter_1 | filter_2 | filter_3
    index_select = df[combined_filter].index
    df.drop(index_select, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #join with growth dim
    selected_cols = ["Sell Down", "CLUSTER", "SIM CODE"]
    df = df.merge(df_dimgrowth[selected_cols], how="left", left_on="Partner SIM", right_on="SIM CODE")
    df.drop("SIM CODE", axis=1, inplace=True)

    df = df.drop_duplicates(subset="Partner SIM").reset_index(drop=True)

    return df


def transform_levels(df, df_extramappings_bu21):
    #drop rows where Level ID is null
    nulls = df[df["Level ID"].isnull()].index
    df.drop(nulls, inplace=True)
    df.reset_index(drop=True, inplace=True)

    #drop cols
    col_drop = [
        "Unnamed: 0",
        "Parent 6 Level Short",
        "Parent 6 Level Name",
        "Parent 7 Level Short",
        "Parent 7 Level Name",
        "Parent 5 Level Short",
        "Parent 5 Level Name",
        "Parent 2 Level Short",
        "Parent 3 Level Short",
        "Parent 4 Level Short",
        "Parent Level Short.1",
        "Parent Level Name.1"
    ]

    df.drop(col_drop, axis=1, inplace=True)

    #add cols
    df["Group Level"] = "EDPR"

    #conditional column - Platform Level
    condition_3 = np.where(
        df["Parent Level Name"] == "EDPR",
        "",
        df["Parent Level Name"]
    )

    condition_2 = np.where(
        df["Parent 2 Level Name"] == "EDPR",
        condition_3,
        df["Parent 2 Level Name"]
    )

    condition_1 = np.where(
        df["Parent 3 Level Name"] == "EDPR",
        condition_2,
        df["Parent 3 Level Name"]
    )

    df["Platform Level"] = np.where(
        df["Parent 4 Level Name"] == "EDPR",
        condition_1,
        df["Parent 4 Level Name"]
    )

    #conditional column - Country Level

    condition_2 = np.where(
        df["Parent 2 Level Name"] == "EDPR",
        "",
        df["Parent Level Name"]
    )

    condition_1 = np.where(
        df["Parent 3 Level Name"] == "EDPR",
        condition_2,
        df["Parent 2 Level Name"]
    )

    df["Country Level"] = np.where(
        df["Parent 4 Level Name"] == "EDPR",
        condition_1,
        df["Parent 3 Level Name"]
    )
    
    #conditional column - Country Special Level

    condition_1 = np.where(
        df["Parent 3 Level Name"] == "EDPR",
        "",
        df["Parent Level Name"]
    )

    df["Country Special Level"] = np.where(
        df["Parent 4 Level Name"] == "EDPR",
        condition_1,
        df["Parent 2 Level Name"]
    )
    
    #conditional column - Country Special Level
    df["Company Level"] = np.where(
        df["Parent 4 Level Name"] == "EDPR",
        "",
        df["Parent Level Name"]
    )

    df_return = pd.concat([df, df_extramappings_bu21]).reset_index(drop=True)
    return df_return

def transform_consoflag(df):
    #drop_cols
    drop_cols = [
        "Level Type",
        "Country",
        "Company Code",
        "Account Code",
        "CostCentre",
        "Partner",
        "codeAcc",
        "intercoAccount", 
        "platformAccount",
        "FlowAccount",
        "BSSourceAccount",
        "Currency",
        "Account Name",
        "Rolls up to",
        "Park",
        "Cash Pooling",
        "Link or Calculated"
    ]
    df.drop(drop_cols, inplace=True, axis=1)

    #rename and change type
    df.rename(columns={"Amount", "ConsoFlag"}, inplace=True)

    #drop flags=0
    index_drop = df[df.ConsoFlag!=1].index
    df.drop(index_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)
    df["Period_Level"] = df.Level + "_" + df.Period.dt.strftime("%Y-%m")

    return df

def transform_deconsolidation(df, df_consoflag):
    df_consoflag_filtered = df_consoflag[df_consoflag.Amount==1].copy()
    df_consoflag_filtered.reset_index(drop=True, inplace=True)
    df = df.merge(df_consoflag_filtered[["Period", "Level"]], how="left", left_on="LevelName", right_on="Level")
    df.drop("Level", axis=1, inplace=True)
    df.rename(columns={"Period": "Sell_Down_Period"}, inplace=True)
    #filter rows
    nulls = df["Sell_Down_Period"].isnull()
    filter_1 = df["Sell_Down_Period"] == "2019-12-01"
    filter_2 = df["Sell_Down_Period"] == "2020-01-01"
    index_drop = df[nulls | filter_1 | filter_2].index
    df.drop(index_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    #conditional column
    df["F98_Account"] = np.where(
        (df["Sell_Down_Period"].dt.year == df["dataPeriod"].dt.year) & (df["Sell_Down_Period"].dt.month >= df["dataPeriod"].dt.month),
        "F98",
        ""
    )

    #drop rows
    nulls = df.FlowAccount.isnull()
    empty_string = df.FlowAccount == ""
    filter_1 = df.F98_Account != "F98"
    index_drop = df[nulls | empty_string | filter_1].index
    df.drop(index_drop, inplace=True)
    df.reset_index(drop=True, inplace=True) 
    #add column
    df["Multiplication"] = df["LC_Amount"].multiply(-1)

    #drop columns
    col_drop = ["dataPeriod", "FlowAccount", "LC_Amount", "D_AU"]
    df.drop(col_drop, axis=1, inplace=True)

    #add column
    df["D_AU"] = "1RET10"

    #create dataframe with dates
    dates = [
    "2020-09-01",
    "2020-10-01",
    "2020-12-01",
    "2021-11-01",
    "2021-12-01",
    "2022-10-01",
    "2022-12-01",
    "2023-12-01",
    "2024-12-01",
    "2025-12-01",
    ]

    list_concat = []
    for date in dates:
        date_datetime = pd.to_datetime(date)
        year= date_datetime.year
        list_dates = [date_datetime]
        extend_dates = pd.date_range(date, periods=len([*range(year, 2025)]), freq='YS', closed="right")
        list_dates.extend(extend_dates)
        df_dates = pd.DataFrame({
            "Date": list_dates,
            "Sell_Down_Date": len(list_dates)*[date_datetime]
        })

        df_dates["Flow"] = np.where(df_dates.Date==date_datetime, "F98", "F00")
        list_concat.append(df_dates.copy())

    df_dates_concat = pd.concat(list_concat, ignore_index=True)
    # df_dates = df_dates.astype({"Sell_Down_Date": "datetime64[ns]"})
    
    #merge dataframes
    df.to_csv("../output/df_pre_merge.csv")
    df_dates_concat.to_csv("../output/df_dates.csv")
    df = df.merge(df_dates_concat, how="left", left_on="Sell_Down_Period", right_on="Sell_Down_Date")
    df.drop("Sell_Down_Date", axis=1, inplace=True)
    df.rename(columns={"Date": "dataPeriod", "Flow": "FlowAccount", "Multiplication": "LC_Amount"}, inplace=True)
    #drop columns
    drop_cols = ["Sell_Down_Period", "F98_Account"]
    df.drop(drop_cols, inplace=True, axis=1)

    return df

def transform_dimlevels_pl(df):
    filter_1 = df["Level_type"].isin(["Non-park", "Park"])
    filter_2 = df["Parent 2 Level Name"] == "Discontinued"
    index_drop = df[~filter_1 | filter_2].index

    df.drop(index_drop, inplace=True)
    df.reset_index(drop=True, inplace=True)

    keep_cols = ["Lavel Name", "Company"]
    drop_cols = [col for col in df.columns if col not in keep_cols]

    df.drop(drop_cols, axis=1, inplace=True)

    df.drop_duplicates(subset=["Company"], inplace=True, ignore_index=True)

    return df

def  transform_fc20_pl(df, df_dimlevels_pl):
    df["D_RU2"] = np.where(df["D_RU"].str.endswith(("MEP", "OLD", "EM")), df.D_RU.str[:4], df.D_RU)
    df = pd.merge(df, df_dimlevels_pl[["Company", "Lavel Name"]], left_on="D_RU", right_on="Company", how="left")
    df.rename(columns={"Lavel Name": "LevelName"}, inplace=True)
    df.drop(["Company", "D_RU", "D_RU2"], axis=1, inplace=True)

    df["D_SC"] = adaptive_version
    df["Period_Level"] = df["LevelName"] + "_" + df.dataPeriod.dt.strftime("%Y_%m")
    df["Partner_Level"] = df["LevelName"] + "_" + df.dataPeriod.dt.strftime("%Y_%m")

    return df

def transform_mappingsim(path):
    #df sim
    df_sim = pd.read_excel(path, sheet_name="SAP", skiprows=[0], dtype=dtypes["sim"])
    keep_rows = ["Conta", "Descritivo EN", "Conta.1", "Filtro w/Detail"]
    drop_rows = [col for col in df_sim.columns if col not in keep_rows]
    df_sim.drop(drop_rows, axis=1, inplace=True)
    df_missing = pd.read_excel(path, sheet_name="Missing_Adaptive", skiprows=[0], dtype=dtypes["missing"])
    drop_cols = ["Descritivo PT", "Descritivo PT.1", "Descritivo EN.1", "DF", "Rubrica 1", "Rubrica 2", "Filtro"]
    
    #df missing
    df_missing.drop(drop_cols, axis=1, inplace=True)
    df_mgtd = pd.read_excel(path, sheet_name="MGTD")
    keep_rows = ["FS", "Filter", "Account", "Account Description EN"]
    drop_rows = [col for col in df_mgtd.columns if col not in keep_rows]
    
    #df mgtd
    df_mgtd.drop(drop_rows, axis=1, inplace=True)
    df_mgtd_bsdetail = df_mgtd[df_mgtd.FS=="Balance - detail"].copy().reset_index(drop=True)
    df_mgtd = pd.merge(df_mgtd, df_mgtd_bsdetail[["Account", "Filter"]], on="Account", how="left")
    df_mgtd.rename(columns={"Filter_x": "Filter", "Filter_y": "BS_detail.Filter"}, inplace=True)
    filter_1 = df_mgtd.FS == "Balance - detail"
    index_drop = df_mgtd[filter_1].index
    df_mgtd.drop(index_drop, inplace=True)
    df_mgtd.reset_index(drop=True, inplace=True)
    notnull = df_mgtd["BS_detail.Filter"].notnull()
    notna = df_mgtd["BS_detail.Filter"] != "n.a."
    df_mgtd["Filtro w/Detail"] = np.where((notnull & notna), df_mgtd["BS_detail.Filter"], df_mgtd["Filter"])
    df_mgtd.rename(columns={"Account": "Conta", "Account Description EN": "Descritivo EN"}, inplace=True)
    df_mgtd["Conta.1"] = df_mgtd["Conta"]
    col_drop = ["FS", "Filter", "BS_detail.Filter"]
    df_mgtd.drop(col_drop, axis=1, inplace=True)
    
    #df concat
    df_concat = pd.concat([df_sim, df_missing, df_mgtd]).reset_index(drop=True)
    df_concat.rename(columns={"Descritivo EN": "Short Name", "Conta": "Code", "Filtro w/Detail": "Filter", "Conta.1": "Magnitude"}, inplace=True)
    df_concat["AccountName"] = df_concat["Short Name"]
    df_concat["P&L"] = np.where(df_concat["Magnitude"].str.startswith("R"), "P&L", "Balance")
    df_concat.drop_duplicates(subset="Code", inplace=True, ignore_index=True)
    index_drop = df_concat[df_concat.Code.isnull()].index
    df_concat.drop(index_drop, inplace=True)
    df_concat.reset_index(inplace=True, drop=True)

    return df_concat

def excel_to_datetime(excel_date):
    a = xlrd.xldate.xldate_as_tuple(int(excel_date), 0)
    return datetime.datetime(*a).strftime("%Y-%m-%d")

def transform_fx(df_fx):
    df_fx["Date"] = np.where(df_fx["Date"].str.fullmatch(r"\d{5}"), df_fx["Date"].map(lambda x: excel_to_datetime(x)), df_fx["Date"])
    df_fx["Date"] = df_fx["Date"].astype("datetime64[ns]")

    filter_1 = df_fx["Scenario"] != adaptive_version
    index_drop = df_fx[filter_1].index
    df_fx = df_fx.drop(index_drop).reset_index(drop=True)

    df_fx.drop(["Scenario", "FX_Key"], axis=1, inplace=True)

    df_fx["Period_FX"] = df_fx["Currency"] + "_" + df_fx.Date.dt.strftime("%Y")

    return df_fx



def transform_load(df, df_dimlevels, df_sim, df_fx, df_cecoslist):
    df.rename(columns={"dataPeriod": "D_PE", "codeAcc": "G/L Account", "FlowAccount": "D_FL", "LC_Amount": "LC_AMOUNT", "Scope": "D_SP", "Scope_T1": "D_SP_T1"}, inplace=True)
    col_drop = ["intercoAccount", "Partner_Level", "Period_Partner", "Period_Level", "AccountName"]
    df.drop(col_drop, axis=1, inplace=True)
    
    s1 = set(df["LevelName"].unique())
    s2 = set(df_dimlevels["Lavel Name"].unique())
    print(s1.difference(s2), "**********************")

    df = pd.merge(df, df_dimlevels[["Lavel Name", "Level Currency", "Company"]], left_on="LevelName", right_on="Lavel Name", how="left")
    df.rename(columns={"Level Currency": "D_CU", "Company": "D_RU"}, inplace=True)
    df.drop("Lavel Name", axis=1, inplace=True)

    df = pd.merge(df, df_sim[["Code", "Magnitude", "P&L"]], left_on="G/L Account", right_on="Code", how="left")
    df.rename(columns={"Magnitude": "D_AC"}, inplace=True)
    df.drop("Code", axis=1, inplace=True)

    df["Period_FX"] = df["D_CU"] + "_" + df.D_PE.dt.strftime("%Y")

    df = pd.merge(df, df_fx[["Period_FX", "FX RATE FINAL", "FX RATE YTD AVG"]], how="left", on="Period_FX")
    
    df["FX"] = np.where(df["P&L"] == "P&L", df["FX RATE YTD AVG"], df["FX RATE FINAL"])
    df["EUR_Amount"] = df["LC_AMOUNT"] / df["FX"]

    partner_values = [
        "Partner",
        "Partner_CH",
        "Partner_EU",
        "Partner_OF",
        "External",
        "uncategorized",
        "Uncategorized",
        "Third party"
    ]

    df["T1"] = np.where(df["Partner"].isin(partner_values), "S9999", df["Partner"])
    df["Source"] = "Adaptive"
    df["Level_CeCo"] = df["LevelName"] + "_" + df["CostCentre"]

    df = pd.merge(df, df_cecoslist[["LevelCeCo", "Profit Center"]], how="left", left_on="Level_CeCo", right_on="LevelCeCo")
    df.drop("LevelCeCo", axis=1, inplace=True)
    df.rename(columns={"Profit Center": "Profit Center_pre"}, inplace=True)
    df.to_csv("semifinal.csv", index=False)
    df["Profit Center"] = np.where(df["Profit Center_pre"].notnull(), df["Profit Center_pre"], df["D_RU"]+"0000")
    df.drop(["P&L", "Period_FX", "FX RATE FINAL", "FX RATE YTD AVG", "Partner", "Level_CeCo", "Profit Center_pre"], axis=1, inplace=True)
    
    return df 
