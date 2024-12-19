import streamlit as st
import pandas as pd
import plotly.express as px
from streamlit_plotly_events import plotly_events
import numpy as np
import datetime

import warnings
warnings.filterwarnings("ignore")




import pymysql
import pandas as pd
import pytz
def call_main_data():




    # AWS and MySQL configuration
    # RDS_HOST = 'gcbdallas.caqfykoqtrvk.us-east-1.rds.amazonaws.com'
    # RDS_USER = 'Dallas_2024'
    # RDS_PASSWORD = 'GCBDallas$223'
    # RDS_DATABASE = 'EMETracking'


    RDS_HOST = '54.218.34.106'
    RDS_USER = 'admin'
    RDS_PASSWORD = 'Root@123'
    RDS_DATABASE = 'EMETracking'

    connection = pymysql.connect(
        host=RDS_HOST,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DATABASE
    )

    # if connection.is_connected():
    print(f"Connected to {RDS_DATABASE}")

    # Get cursor
    cursor = connection.cursor()
    cursor.execute("select * from eme_status")
    status_data = cursor.fetchall()

    import pandas as pd
    stcolumn_names = list(map(lambda x: x[0], cursor.description))
    status_data=pd.DataFrame(status_data,columns=stcolumn_names)
    print("status_data : ",len(status_data))
    # status_data


    import pandas as pd
    import ast  # To safely evaluate string representations of Python literals

    df = pd.DataFrame(status_data)

    # Function to safely convert string representations of lists into actual lists
    def safe_literal_eval(value):
        try:
            return ast.literal_eval(value) if value is not None else []
        except (ValueError, SyntaxError):
            return []

    # Convert the string representations of lists in ListColumn1 into actual lists
    df['final_delivery_date'] = df['final_delivery_date'].apply(safe_literal_eval)
    df['final_status'] = df['final_status'].apply(safe_literal_eval)
    df['final_delivery_by'] = df['final_delivery_by'].apply(safe_literal_eval)

    # Exploding ListColumn1
    df_exploded = df.explode('final_status').reset_index(drop=True)
    df_exploded = df_exploded.explode('final_delivery_date').reset_index(drop=True)
    df_exploded = df_exploded.explode('final_delivery_by').reset_index(drop=True)

    # Ensure ListColumn2 and OtherColumn are correctly aligned after explosion
    # We are duplicating ListColumn2 and OtherColumn to match the exploded rows
    # df_exploded[3] = df_exploded.groupby(0)[3].transform('first')
    # df_exploded[4] = df_exploded.groupby(0)[4].transform('first')

    # df_exploded



    df_last = df_exploded.drop_duplicates(subset='id', keep='last').reset_index(drop=True)

    # df_last

    # Execute the query to fetch data
    cursor.execute("SELECT * FROM eme_emerequest")
    reqdata = cursor.fetchall()
    rdcolumn_names = list(map(lambda x: x[0], cursor.description))
    # print("rdcolumn_names : ",rdcolumn_names)
    reqdatadf = pd.DataFrame(reqdata, columns=rdcolumn_names)
    # reqdatadf = len(reqdatadf)
    # reqdatadf.tail(10)
    # getdfmnf2 = pd.merge(df_last,  reqdatadf[['id', 'request_id','request_creation_date','po_received','carrier','site_id']], left_on='request_id_id', right_on='id', how='left')
    getdfmnf2 = pd.merge(df_last, reqdatadf[['id', 'request_id','request_creation_date','po_received','carrier','site_id']], left_on='request_id_id', right_on='id', how='outer')


    print(len(reqdatadf))
    print(len(getdfmnf2))

    # Execute the query to fetch data
    cursor.execute("SELECT * FROM eme_trackingassignee")
    asidata = cursor.fetchall()
    asicolumn_names = list(map(lambda x: x[0], cursor.description))
    # print("rdcolumn_names : ",rdcolumn_names)
    asidatadf = pd.DataFrame(asidata, columns=asicolumn_names)
    # asidatadf.columns

    asidatadf = asidatadf.dropna(subset=['request_id_id'])

    getdfmnf3 = pd.merge(getdfmnf2, asidatadf[['request_id_id', 'Inventory_Submission_Date','Simulatiion_Submission_Date','Mitigation_Submission_Date','report_generation_Submission_Date','Quality_Check_Submission_Date','Exceedance_Updates_Submission_Date']], left_on='id_y', right_on='request_id_id', how='outer')

    getdfmnf3 = getdfmnf3.dropna(subset=['id_y'])
    # getdfmnf3.tail(2)

    getdfmnf3 = getdfmnf3[getdfmnf3['final_status']=="Pending"]


    totaldf=[]
    tasksublist ={'Inventory':'Inventory_Submission_Date','Simulation':'Simulatiion_Submission_Date','Mitigation':'Mitigation_Submission_Date','Report Generation':'report_generation_Submission_Date','Quality Check':'Quality_Check_Submission_Date'}#,'Exceedance/Updates':'Exceedance_Updates_Submission_Date'
    for ktl,vtl in tasksublist.items():
        fdf = {}
        filtered_df = getdfmnf3[getdfmnf3[vtl].isna() | (getdfmnf3[vtl] == '') | (getdfmnf3[vtl] == 'None')]
        filtered_df=filtered_df.rename(columns={'request_creation_date':'Request creation date','request_id':'Request Id','carrier':'Carrier','site_id_y':'Site name','final_delivery_date':'Final Delivery date','po_received':'PO Received'})
        filtered_df = filtered_df[['Request creation date', 'Request Id', 'Carrier', 'Site name','Final Delivery date', 'PO Received', 'final_status']]
        fdf["total"]=len(filtered_df)
        filtered_df.reset_index(inplace=True)
        filtered_df=filtered_df.drop(['index'],axis=1)
        fdf[ktl]=filtered_df
        totaldf.append(fdf)
        # break

    return totaldf




 
data1 = call_main_data()
print("data : ",data1)

tasksublist ={'Inventory':0,'Simulation':1,'Mitigation':2,'Report Generation':3,'Quality Check':4}#,'Exceedance/Updates':5

data = {
    "Category": list(tasksublist.keys()),
    "Sum": [ads['total'] for ads in data1],
    "Details": [ads for ads in data1],
}
df = pd.DataFrame(data)
# st.title("Interactive Bar Chart with Table View")
fig = px.bar(df, x="Category", y="Sum", title="Pending Sites Overview", color="Category")
st.plotly_chart(fig)
selected_category = st.selectbox("Select a Category:", df["Category"])
print("selected_category  1: ",selected_category)
print("selected_category  2: ",tasksublist[selected_category])
if selected_category:
    selected_index = pd.DataFrame()
    for dd in data1:
        print("dd.keys() : ",dd.keys())
        if selected_category in dd.keys():
            selected_index = dd[selected_category] 
    selected_index = pd.DataFrame(selected_index)


        # Convert DataFrame to Excel in-memory
    from io import BytesIO  # Import BytesIO for in-memory file handling
    excel_file = BytesIO()  # Create an in-memory bytes buffer
    with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
        selected_index.to_excel(writer, index=False, sheet_name=selected_category.replace('/','_'))
    
    # Seek to the beginning of the buffer before using it
    excel_file.seek(0)
    # Provide the option to download the DataFrame as an Excel file
    st.download_button(
        label="Download Excel",
        data=excel_file,
        file_name=f"{selected_category}_pending_data.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )



    st.write(f"Details for {selected_category}:")
    st.dataframe(selected_index)













# # Prepare data for the pie chart
# status_counts = pd.DataFrame({
#     "status": list(data.keys()),
#     "count": [data[status]["count"] for status in data]
# })

# total_count = status_counts["count"].sum()

# # Create pie chart
# fig = px.pie(
#     status_counts,
#     values="count",
#     names="status",
#     title="Final Status Graph",
#     color="status",
#     hole=0.5,
#     color_discrete_map={
#         "Pending": "blue",
#         "On hold": "orange",
#         "Submitted": "lightgreen",
#         "Not assigned": "black",
#         "Cancelled": "red"
#     }
# )


# # Add total count in the center of the pie chart
# fig.update_layout(
#     annotations=[dict(
#         text=f"<b>{total_count}</b>",
#         x=0.5,
#         y=0.5,
#         font_size=30,
#         showarrow=False
#     )]
# )


# # Display the interactive chart and capture click events
# # st.write("### Click on a status in the graph to see details:")
# selected_points = plotly_events(fig, click_event=True)

# # Process click event
# if selected_points:
#     clicked_point_index = selected_points[0].get("pointNumber")
#     if clicked_point_index is not None and clicked_point_index < len(status_counts):
#         clicked_status = status_counts.iloc[clicked_point_index]["status"]
#         # st.write(f"### Showing data for status: `{clicked_status}`")

#         excel_file_data = data[clicked_status]["df"]
#          # Convert DataFrame to Excel in-memory
#         from io import BytesIO  # Import BytesIO for in-memory file handling
#         excel_file = BytesIO()  # Create an in-memory bytes buffer
#         with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
#             excel_file_data.to_excel(writer, index=False, sheet_name=clicked_status)
        
#         # Seek to the beginning of the buffer before using it
#         excel_file.seek(0)
#         # Provide the option to download the DataFrame as an Excel file
#         st.download_button(
#             label="Download Excel",
#             data=excel_file,
#             file_name=f"{clicked_status}_data.xlsx",
#             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
#         )

#         st.dataframe(data[clicked_status]["df"])  # Display the corresponding DataFrame

#     else:
#         st.error("Could not identify the clicked status. Please try again.")
# else:
#     # pass
#     st.caption("### Click on a section of the pie chart to view details.")
