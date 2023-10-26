import os
import pandas as pd


def separate_data():
    folder_names = ["h_8186_q_8186", "h_200_q_005", "h_blank_q_mess"]

    for folder_name in folder_names:
        path = os.path.join('static', folder_name)
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"'{folder_name}' has been created.")
        else:
            print(f"'{folder_name}' already exists.")

    df_h = pd.read_excel("./static/hisense_file.xlsx", converters={"Reference": str, "Material Document": str})
    series_temp = df_h["Storage Location"]
    series_temp_grad = series_temp.map(
        {"VQ00": "NEW", "VQ03": "NEW", "VQ04": "NEW", "VQ05": "NEW", "VQ02": "FAULTY", "VQ01": "CTNDAM"})
    df_h["Grade"] = series_temp_grad
    df_h["Reference"] = df_h["Reference"].fillna("blank")
    df_h_8186 = df_h[(df_h['Reference'].str.startswith('0081')) | (df_h['Reference'].str.startswith('086'))]
    df_h_200 = df_h[df_h['Reference'].str.startswith('200')]
    df_h_blank = df_h[df_h['Reference'] == "blank"]
    df_h_8186.to_csv("./static/h_8186_q_8186/df_h_8186.csv")
    df_h_200.to_csv("./static/h_200_q_005/df_h_200.csv")
    df_h_blank.to_csv("./static/h_blank_q_mess/df_h_blank.csv")

    df_q = pd.read_excel("./static/qls_file.xlsx", converters={"Cust. Reference": str})
    df_q = df_q.fillna({"Grade": "NEW"})
    df_q['Cust. Reference'] = df_q['Cust. Reference'].fillna("blank")
    df_q_8186 = df_q[(df_q['Cust. Reference'].str.startswith('0081')) | (df_q['Cust. Reference'].str.startswith('086'))]
    df_q_005 = df_q[df_q['Cust. Reference'].str.startswith('005')]
    append_idx = df_q_8186.index.to_list() + df_q_005.index.to_list()
    mask = ~df_q.index.isin(append_idx)
    df_q_mess = df_q[mask]
    df_q_8186.to_csv("./static/h_8186_q_8186/df_q_8186.csv")
    df_q_005.to_csv("./static/h_200_q_005/df_q_005.csv")
    df_q_mess.to_csv("./static/h_blank_q_mess/df_q_mess.csv")


def work_200_005():
    df_h_200 = pd.read_csv("./static/h_200_q_005/df_h_200.csv", converters={"Reference": str, "Batch": str},
                             index_col=0)
    h_all_cols = ["Grade", "Material Description", "Batch", "Qty in Un. of Entry"]
    h_group_cols = ["Grade", "Material Description", "Batch"]
    df_h_200_group = df_h_200[h_all_cols].groupby(h_group_cols).sum().reset_index()

    df_q_005 = pd.read_csv("./static/h_200_q_005/df_q_005.csv", index_col=0, converters={"Cust. Reference": str})
    q_all_cols = ["Product", "Grade", "Quantity", "Cust. Reference"]
    q_group_cols = ["Product", "Grade", "Cust. Reference"]
    df_q_005_group = df_q_005[q_all_cols].groupby(q_group_cols).sum().reset_index()

    match_list_1 = []
    double_check_list_1 = []
    for _, row in df_h_200_group.iterrows():

        grade = row["Grade"]
        product = row["Material Description"]

        batch = row["Batch"]
        if len(batch) < 10:
            continue
        else:
            ref = "00" + batch[:8]
        quantity = row["Qty in Un. of Entry"]

        df_q_select = df_q_005_group[
            (df_q_005_group["Grade"] == grade) &
            (df_q_005_group['Product'].isin(product.split("."))) &
            (df_q_005_group["Cust. Reference"] == ref) &
            (df_q_005_group["Quantity"] == quantity)
            ]

        if df_q_select.shape[0] == 1:
            mach_info = {
                "Grade": grade,
                "Product": df_q_select["Product"].iloc[0],
                "Reference": ref,
                "Batch": batch,
                "Material Description": product
            }
            match_list_1.append(mach_info)
        if df_q_select.shape[0] > 1:
            print(product)
            mach_info = {
                "Grade": grade,
                "Product": df_q_select["Product"].iloc[0],
                "Reference": ref,
                "Batch": batch,
                "Material Description": product
            }
            double_check_list_1.append(mach_info)

    if double_check_list_1:
        print("double check work_200_005")
        print(double_check_list_1)

    df_h_200_match = pd.DataFrame(columns=df_h_200.columns)
    for info_dict in match_list_1:
        grade = info_dict["Grade"]
        des = info_dict["Material Description"]
        batch = info_dict["Batch"]

        filter_df = df_h_200[
            (df_h_200["Grade"] == grade) &
            (df_h_200["Material Description"] == des) &
            (df_h_200["Batch"] == batch)
            ]

        df_h_200_match = pd.concat([df_h_200_match, filter_df])

    mask = ~df_h_200.index.isin(df_h_200_match.index)
    df_h_200_diff = df_h_200[mask]

    df_q_005_match = pd.DataFrame(columns=df_q_005.columns)
    for info_dict in match_list_1:
        grade = info_dict["Grade"]
        des = info_dict["Material Description"]
        ref = info_dict["Reference"]
        product = info_dict["Product"]

        filter_df = df_q_005[
            (df_q_005["Grade"] == grade) &
            (df_q_005["Product"] == product) &
            (df_q_005["Cust. Reference"] == ref)
            ]

        df_q_005_match = pd.concat([df_q_005_match, filter_df])

    mask = ~df_q_005.index.isin(df_q_005_match.index)
    df_q_005_diff = df_q_005[mask]

    df_list = [df_q_005_match, df_q_005_diff, df_h_200_match, df_h_200_diff]
    df_list_name = ["df_q_005_match.csv", "df_q_005_diff.csv", "df_h_200_match.csv", "df_h_200_diff.csv"]
    for i in range(len(df_list)):
        path = "./static/h_200_q_005/" + df_list_name[i]
        df_list[i].to_csv(path)


def work_8186():
    df_h_8186 = pd.read_csv("./static/h_8186_q_8186/df_h_8186.csv", converters={"Reference": str}, index_col=0)
    h_all_cols = ["Grade", "Material Description", "Reference", "Qty in Un. of Entry"]
    h_group_cols = ["Grade", "Material Description", "Reference"]
    df_h_8186_group = df_h_8186[h_all_cols].groupby(h_group_cols).sum().reset_index()

    df_q_8186 = pd.read_csv("./static/h_8186_q_8186/df_q_8186.csv", index_col=0)
    q_all_cols = ["Product", "Grade", "Quantity", "Cust. Reference"]
    q_group_cols = ["Product", "Grade", "Cust. Reference"]
    df_q_8186_group = df_q_8186[q_all_cols].groupby(q_group_cols).sum().reset_index()

    match_list_1 = []
    double_check_list_1 = []
    for _, row in df_q_8186_group.iterrows():
        grade = row["Grade"]
        product = row["Product"]
        # set regex
        pat = ".*" + product + "\..*"
        ref = row["Cust. Reference"]
        quantity = row["Quantity"]

        df_h_select = df_h_8186_group[
            (df_h_8186_group["Grade"] == grade) &
            (df_h_8186_group['Material Description'].str.match(pat=pat, case=False)) &
            (df_h_8186_group["Reference"] == ref) &
            (df_h_8186_group["Qty in Un. of Entry"] == quantity)
            ]

        if df_h_select.shape[0] == 1:
            mach_info = {
                "Grade": grade,
                "Product": product,
                "Reference": ref,
                "Material Description": df_h_select["Material Description"].iloc[0]
            }
            match_list_1.append(mach_info)
        if df_h_select.shape[0] > 1:
            print(product)
            mach_info = {
                "Reference": ref,
                "df": df_h_select
            }
            double_check_list_1.append(mach_info)

    if double_check_list_1:
        print("double check work_200_005")
        print(double_check_list_1)

    df_h_8186_match = pd.DataFrame(columns=df_h_8186.columns)
    for info_dict in match_list_1:
        grade = info_dict["Grade"]
        des = info_dict["Material Description"]
        ref = info_dict["Reference"]

        filter_df = df_h_8186[
            (df_h_8186["Grade"] == grade) &
            (df_h_8186["Material Description"] == des) &
            (df_h_8186["Reference"] == ref)
            ]

        df_h_8186_match = pd.concat([df_h_8186_match, filter_df])

    mask = ~df_h_8186.index.isin(df_h_8186_match.index)
    df_h_8186_diff = df_h_8186[mask]

    df_q_8186_match = pd.DataFrame(columns=df_q_8186.columns)
    for info_dict in match_list_1:
        grade = info_dict["Grade"]
        des = info_dict["Material Description"]
        ref = info_dict["Reference"]
        product = info_dict["Product"]

        filter_df = df_q_8186[
            (df_q_8186["Grade"] == grade) &
            (df_q_8186["Product"] == product) &
            (df_q_8186["Cust. Reference"] == ref)
            ]

        df_q_8186_match = pd.concat([df_q_8186_match, filter_df])

    mask = ~df_q_8186.index.isin(df_q_8186_match.index)
    df_q_8186_diff = df_q_8186[mask]

    df_list = [df_q_8186_diff, df_q_8186_match, df_h_8186_match, df_h_8186_diff]
    df_list_name = ["df_q_8186_diff.csv", "df_q_8186_match.csv", "df_h_8186_match.csv", "df_h_8186_diff.csv"]
    for i in range(len(df_list)):
        path = "./static/h_8186_q_8186/" + df_list_name[i]
        df_list[i].to_csv(path)




