import pandas as pd
import networkx as nx

from functools import lru_cache

#//-----------------------Duplicate management----------------------------------

def Duplicate_Management(DataFrame):

    """
    Verify duplicate entries in a dataframe by grouping related records based on Full Name and email attributes 
    and consolidating their information into a single entry.

    Args:
        DataFrame (pd.DataFrame): A pandas DataFrame containing at least the columns 'firstname', 'lastname', 
                           'Email', 'industry', and 'technical_test___create_date'.

    Returns:
        pd.DataFrame: A DataFrame with consolidated rows for duplicate records.

    Description:
        This function groups duplicate records based on shared values in 'FullName' and 'Email'. 
        It creates a new column, 'FullName', combining the 'firstname' and 'lastname'. Then, using 
        a graph, identifies connected components (groups of records with shared names 
        or emails). The 'Group' column is assigned based on these connections. 

        The DataFrame is sorted by 'Group' and 'technical_test___create_date', the 'industry' column values for each group
        are combined into a semicolon-separated(“ ; ”) string, and the first row of each group is filled with non-null values from 
        the other rows in the group if any missing values are present.

        Finally the first row from each group is retained after filling missing values, creating a final result that consolidates all information 
        from the duplicates.

    """

    DataFrame["FullName"] = DataFrame['firstname'] + ' ' + DataFrame['lastname']

    Graph = nx.Graph()

    for index, row in DataFrame.iterrows():
        if pd.notna(row['FullName']):
            Graph.add_edge(f"Name_{row['FullName']}", index)
        if pd.notna(row['Email']):
            Graph.add_edge(f"Email_{row['Email']}", index)

    connected_components = list(nx.connected_components(Graph))

    group_map = {}

    for group_id, component in enumerate(connected_components):
        for node in component:
            if isinstance(node, int): 
                group_map[node] = group_id
    DataFrame['Group'] = DataFrame.index.map(group_map)

    DataFrame['technical_test___create_date'] = pd.to_datetime(DataFrame['technical_test___create_date'])

    DataFrameAux = DataFrame.sort_values(['Group', 'technical_test___create_date'], ascending=[True, False])


    grouped = DataFrameAux.groupby('Group')
    
    Results = []

    for index, (_, Group) in enumerate(grouped):
        Unique_Values = Group['industry'].dropna().unique()
        if len(Unique_Values) > 1:
            Combined = ';' +';'.join(map(str, Unique_Values)) 
        else:
            Combined = ';'.join(map(str, Unique_Values))
        Group = Group.copy()
        Group.iloc[0, Group.columns.get_loc('industry')] = Combined
        
        first_row_Group = Group.iloc[0].isna().any()
        if first_row_Group :
            if len(Group) > 1:
                for i in range(1, len(Group)):
                    Group.iloc[0] = Group.iloc[0].fillna(Group.iloc[i])
                    if not Group.iloc[0].isna().any():
                        break
            Results.append(Group.iloc[0])
        else:
            Results.append(Group.iloc[0])

    Clean_DataFrame = pd.DataFrame(Results).reset_index(drop=True)

    Clean_DataFrame.drop(columns=["FullName","Group"], inplace=True)

    return Clean_DataFrame

#//-----------------------End duplicate management-------------------------------
