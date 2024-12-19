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
def call_main_data(daterange):
    print("daterange ai: ",type(daterange[0]),daterange[0])
    print("daterange ai: ",type(daterange[1]),daterange[1])
    import datetime
    # start_date = datetime.date(daterange[0])
    # end_date = datetime.date(daterange[1])
    start_date = daterange[0]
    end_date = daterange[1]
    print(start_date,end_date)
    # Create a Dash app using DjangoDash instead of Dash
    # app = DjangoDash('DonutChartApp')  # 'DonutChartApp' is the app name



    # # AWS and MySQL configuration
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
    print("status_data columnns",status_data.columns)
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

    getdfmnf2['final_status'] = getdfmnf2['final_status'].fillna('Not assigned')

    getdfmnf2 = getdfmnf2.sort_values(by=['id_x'],ascending=False)
    # getdfmnf2

    getdfmnf2 = getdfmnf2[['request_creation_date','request_id','carrier','site_id_y','final_delivery_date','po_received','final_status','id_y']]







    import pandas as pd
    from datetime import datetime

    # Assuming getdfmnf2 is your DataFrame
    # Apply the transformation to 'po_received' column
    getdfmnf2['po_received'] = getdfmnf2['po_received'].apply(lambda x: None if x in ['-', ' ', 'None', None] else pd.to_datetime(x, errors='coerce').strftime('%Y-%m-%d') if pd.to_datetime(x, errors='coerce') is not pd.NaT else None)

    # Print the updated DataFrame
    try:
        print("getdfmnf2 : ",len(getdfmnf2['po_received']))
        getdfmnf2=getdfmnf2[(getdfmnf2['request_creation_date'] > start_date) & (getdfmnf2['request_creation_date'] < end_date)]
        # getdfmnf2['po_received'].unique()
    except Exception as e:
        print("daterange error : ",e)
        pass





    import pandas as pd
    import datetime
    import datetime as dt

    getdfmnf2['po_received'] = pd.to_datetime(getdfmnf2['po_received'], errors='coerce')
    getdfmnf2['request_creation_date'] = pd.to_datetime(getdfmnf2['request_creation_date'], errors='coerce')
    current_date=datetime.datetime.now(pytz.timezone('US/Central')).strftime('%Y-%m-%d')
    current_date = pd.to_datetime(current_date)
    # Get the current date and time in UTC, then apply the CST offset
    # Calculate 'Aging' by subtracting 'po_received' from the current date
    # getdfmnf2['Aging'] = getdfmnf2['po_received'].apply(lambda x: (current_date - x).days if pd.notna(x) else None)
    getdfmnf2['Aging'] = getdfmnf2['request_creation_date'].apply(lambda x: (current_date - x).days if pd.notna(x) else None)

    # print(getdfmnf2)



    getdfmnf2['po_received'] = getdfmnf2['po_received'].apply(str)

    getdfmnf2=getdfmnf2.rename(columns={'request_creation_date':'Request creation date','request_id':'Request Id','carrier':'Carrier','site_id_y':'Site name','final_delivery_date':'Final Delivery date','po_received':'PO Received'})
    # Ensure 'Request creation date' is in datetime format (if it's not already)
    getdfmnf2['Request creation date'] = pd.to_datetime(getdfmnf2['Request creation date'], errors='coerce')
    getdfmnf2 = getdfmnf2.sort_values(by='Request creation date', ascending=False)
    # getdfmnf2
    getdfmnf2['Request creation date']=getdfmnf2['Request creation date'].apply(str)

    getdfmnf2.reset_index(inplace=True)
    getdfmnf2=getdfmnf2.drop(['index'],axis=1)
    # getdfmnf2

    try:
        # getdfmnf2.columns
        getdfmnf3 = getdfmnf2.copy()
        getdfmnf2 = getdfmnf2[['Request creation date', 'Request Id', 'Carrier', 'Site name','Final Delivery date', 'PO Received', 'final_status', 'Aging']]
        print(len(getdfmnf2[getdfmnf2['final_status'] == 'On hold']))

        getdfmnf4 = pd.merge(getdfmnf3, status_data[['request_id_id','final_status']], left_on='id_y', right_on='request_id_id', how='outer')

        getdfmnf5 = getdfmnf4[getdfmnf4['final_status_x'] == 'On hold']
        print(len(getdfmnf4))
        print(len(getdfmnf5))
        getdfmnf5['count(on-holds)'] = getdfmnf5['final_status_y'].apply(lambda x: x.count("On hold") if x else 0)
        # getdfmnf5
        # getdfmnf5=getdfmnf5.drop(columns=['final_status_y'],axis=1)
        getdfmnf5=getdfmnf5.drop(columns=['final_status_y','request_id_id','id_y'],axis=1)
        getdfmnf5.rename(columns={'final_status_x':"Final Status"},inplace=True)
    except Exception as e:
        print("error count(on-holds)",e)
        getdfmnf5['count(on-holds)']=0
        pass



    
    submitted_df = getdfmnf2[getdfmnf2['final_status'] == 'Submitted']
    # onhold_df = getdfmnf2[getdfmnf2['final_status'] == 'On hold']
    onhold_df = getdfmnf5
    cancelled_df = getdfmnf2[getdfmnf2['final_status'] == 'Cancelled']
    pending_df = getdfmnf2[getdfmnf2['final_status'] == 'Pending']
    not_assigned = getdfmnf2[getdfmnf2['final_status'] == 'Not assigned']

    try:
        # Ensure both columns are in datetime format
        submitted_df['Final Delivery date'] = pd.to_datetime(submitted_df['Final Delivery date'], errors='coerce')
        submitted_df['PO Received'] = pd.to_datetime(submitted_df['PO Received'], errors='coerce')
        submitted_df['Request creation date'] = pd.to_datetime(submitted_df['Request creation date'], errors='coerce')
        
        # submitted_df['Aging'] = abs((submitted_df['Final Delivery date'] - submitted_df['PO Received']).dt.days)
        submitted_df['Aging'] = abs((submitted_df['Final Delivery date'] - submitted_df['Request creation date']).dt.days)
        # submitted_df = submitted_df.dropna(subset=['Final Delivery date', 'PO Received'])
        submitted_df = submitted_df.fillna('0')
        # submitted_df['PO Received'].apply(str)
        # submitted_df=submitted_df['PO Received'].apply(str)
        submitted_df['PO Received']=submitted_df['PO Received'].apply(str)
        submitted_df['Final Delivery date']=submitted_df['Final Delivery date'].apply(str)
        submitted_df.reset_index(inplace=True)
        submitted_df=submitted_df.drop(['index'],axis=1)

        # submitted_df
    except:
        pass





    print(len(submitted_df))
    print(len(onhold_df))
    print(len(cancelled_df))
    print(len(pending_df))
    print(len(not_assigned))



    # Assuming df_last is your DataFrame and you are working with column 2
    unique_values = getdfmnf2['final_status'].unique()

    # Initialize a dictionary to store the unique value and its corresponding length
    unique_lengths = {value: len(str(value)) for value in unique_values}

    # Now let's also count the occurrences of each unique value
    value_counts = getdfmnf2['final_status'].value_counts()

    # Combine the unique values with their counts and lengths
    result = {value: {'count': value_counts[value], 'length': unique_lengths[value]} for value in unique_values}

    # Print the result
    # print(result)





    try:
        result['Submitted']['df']=submitted_df
    except:
        result['Submitted']={}
        result['Submitted']['count']=0
        result['Submitted']['df']=pd.DataFrame()    
        
    try:
        result['On hold']['df']=onhold_df
    except:
        result['On hold']={}
        result['On hold']['count']=0
        result['On hold']['df']=pd.DataFrame()
        
    try:
        result['Cancelled']['df']=cancelled_df
    except:
        result['Cancelled']={}
        result['Cancelled']['count']=0
        result['Cancelled']['df']=pd.DataFrame()

    try:
        result['Pending']['df']=pending_df
    except:
        result['Pending']={}
        result['Pending']['count']=0
        result['Pending']['df']=pd.DataFrame()

    try:
        result['Not assigned']['df']=not_assigned
    except:
        result['Not assigned']={}
        result['Not assigned']['count']=0
        result['Not assigned']['df']=pd.DataFrame()
    # result
    return result




daterange=None

today = datetime.datetime.now().date()
start_date = datetime.date(today.year, 1, 1)
end_date = today
daterange = st.date_input(
    "Select date range ",
    (start_date, end_date),  # Default to the whole year until today
    min_value=start_date,
    max_value=end_date,
    format="MM.DD.YYYY",  # Date format
)
print("daterange : ",daterange)




data = call_main_data(daterange)

# Prepare data for the pie chart
status_counts = pd.DataFrame({
    "status": list(data.keys()),
    "count": [data[status]["count"] for status in data]
})

total_count = status_counts["count"].sum()

# Create pie chart
fig = px.pie(
    status_counts,
    values="count",
    names="status",
    title="Final Status Graph",
    color="status",
    hole=0.5,
    color_discrete_map={
        "Pending": "blue",
        "On hold": "orange",
        "Submitted": "lightgreen",
        "Not assigned": "black",
        "Cancelled": "red"
    }
)


# Add total count in the center of the pie chart
fig.update_layout(
    annotations=[dict(
        text=f"<b>{total_count}</b>",
        x=0.5,
        y=0.5,
        font_size=30,
        showarrow=False
    )]
)


# Display the interactive chart and capture click events
# st.write("### Click on a status in the graph to see details:")
selected_points = plotly_events(fig, click_event=True)

# Process click event
if selected_points:
    clicked_point_index = selected_points[0].get("pointNumber")
    if clicked_point_index is not None and clicked_point_index < len(status_counts):
        clicked_status = status_counts.iloc[clicked_point_index]["status"]
        # st.write(f"### Showing data for status: `{clicked_status}`")

        excel_file_data = data[clicked_status]["df"]
         # Convert DataFrame to Excel in-memory
        from io import BytesIO  # Import BytesIO for in-memory file handling
        excel_file = BytesIO()  # Create an in-memory bytes buffer
        with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
            excel_file_data.to_excel(writer, index=False, sheet_name=clicked_status)
        
        # Seek to the beginning of the buffer before using it
        excel_file.seek(0)
        # Provide the option to download the DataFrame as an Excel file
        st.download_button(
            label="Download Excel",
            data=excel_file,
            file_name=f"{clicked_status}_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.dataframe(data[clicked_status]["df"])  # Display the corresponding DataFrame

    else:
        st.error("Could not identify the clicked status. Please try again.")
else:
    # pass
    st.caption("### Click on a section of the pie chart to view details.")
