import streamlit as st 
from streamlit_option_menu import option_menu 
import psycopg2
import pandas as pd
import plotly.express as px
import requests
import json
from PIL import Image
import base64




#data frame creation for the tables in posgress sql

#sql connection

mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      port="5432",
                      database="Phonepe",
                      password="12345")
cursor=mydb.cursor()

#dataframe creation for aggregated insurance table

cursor.execute("SELECT * FROM agg_insurance")
mydb.commit()
table1=cursor.fetchall()

Agg_insurance=pd.DataFrame(table1,columns=("States","Years","Quarter","Transaction_type",
                                           "Transaction_count","Transaction_amount"))

#dataframe creation for aggregated transaction table

cursor.execute("SELECT * FROM agg_transaction")
mydb.commit()
table2=cursor.fetchall()

Agg_transaction=pd.DataFrame(table2,columns=("States","Years","Quarter","Transaction_type",
                                           "Transaction_count","Transaction_amount"))



#dataframe creation for aggregated user table

cursor.execute("SELECT * FROM agg_user")
mydb.commit()
table3=cursor.fetchall()

Agg_user=pd.DataFrame(table3,columns=("States","Years","Quarter","Brands",
                                           "Transaction_count","Percentage"))

#dataframe creation for map insurance table

cursor.execute("SELECT * FROM map_insurance")
mydb.commit()
table4=cursor.fetchall()

Map_insurance=pd.DataFrame(table4,columns=("States","Years","Quarter","District",
                                           "Transaction_count","Transaction_amount"))


#dataframe creation for map transaction table

cursor.execute("SELECT * FROM map_transaction")
mydb.commit()
table5=cursor.fetchall()

Map_transaction=pd.DataFrame(table5,columns=("States","Years","Quarter","District",
                                           "Transaction_count","Transaction_amount"))


#dataframe creation for map user table

cursor.execute("SELECT * FROM map_user")
mydb.commit()
table6=cursor.fetchall()

Map_user=pd.DataFrame(table6,columns=("States","Years","Quarter","District",
                                           "RegisteredUsers","AppOpens"))


#dataframe creation for top insurance table

cursor.execute("SELECT * FROM top_insurance")
mydb.commit()
table7=cursor.fetchall()

Top_insurance=pd.DataFrame(table7,columns=("States","Years","Quarter","Pincodes",
                                           "Transaction_count","Transaction_amount"))

#dataframe creation for top transaction table

cursor.execute("SELECT * FROM top_transaction")
mydb.commit()
table8=cursor.fetchall()

Top_transaction=pd.DataFrame(table8,columns=("States","Years","Quarter","Pincodes",
                                           "Transaction_count","Transaction_amount"))


#dataframe creation for top user table

cursor.execute("SELECT * FROM top_user")
mydb.commit()
table9=cursor.fetchall()

Top_user=pd.DataFrame(table9,columns=("States","Years","Quarter","Pincodes",
                                           "RegisteredUsers"))



def Transaction_amount_count_Y(df,year): #dataframe and year of transaction amt and tran count
    tran_amt_cnt_yr=df[df["Years"]==year]
    tran_amt_cnt_yr.reset_index(drop=True,inplace=True)

    tran_amt_cnt_y_grp=tran_amt_cnt_yr.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tran_amt_cnt_y_grp.reset_index(inplace=True)

    col1,col2=st.columns(2)
    with col1:
        

        fig_amount=px.bar(tran_amt_cnt_y_grp, x="States",y="Transaction_amount",title=f"Transaction_amount--{year}",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height=650,width=600)
        st.plotly_chart(fig_amount)
    
    with col2:

        fig_count=px.bar(tran_amt_cnt_y_grp, x="States",y="Transaction_count",title=f"Transaction_count--{year}",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650,width=600)
        st.plotly_chart(fig_count)
    
    col1,col2=st.columns(2)
    with col1:
        
        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(url)
        data1=json.loads(response.content)
        states_name=[]
        for data in data1["features"]:
            states_name.append(data["properties"]["ST_NM"])
            
        states_name.sort()

        fig_india_1=px.choropleth(tran_amt_cnt_y_grp,geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_amount",color_continuous_scale="thermal",
                                range_color=(tran_amt_cnt_y_grp["Transaction_amount"].min(),tran_amt_cnt_y_grp["Transaction_amount"].max()),
                                hover_name="States", title=f"{year} TRANSACTION AMOUNT",fitbounds="locations",
                                height=700,width=650)
        
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)
    
    with col2:    
        
        fig_india_2=px.choropleth(tran_amt_cnt_y_grp,geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_count",color_continuous_scale="thermal",
                                range_color=(tran_amt_cnt_y_grp["Transaction_count"].min(),tran_amt_cnt_y_grp["Transaction_count"].max()),
                                hover_name="States", title=f"{year} TRANSACTION COUNT",fitbounds="locations",
                                height=700,width=650)
        
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)
        
    return tran_amt_cnt_yr


def Transaction_amount_count_Y_Q(df,quarter): #dataframe of tran amt and count for quarter
    tran_amt_cnt_yr=df[df["Quarter"]==quarter]
    tran_amt_cnt_yr.reset_index(drop=True,inplace=True)

    tran_amt_cnt_y_grp=tran_amt_cnt_yr.groupby("States")[["Transaction_count","Transaction_amount"]].sum()
    tran_amt_cnt_y_grp.reset_index(inplace=True)

    col1,col2=st.columns(2)
    with col1:
        
        fig_amount=px.bar(tran_amt_cnt_y_grp, x="States",y="Transaction_amount",title=f" TRANSACTION AMOUNT--{tran_amt_cnt_yr["Years"].min()} QUARTER {quarter}",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height=650,width=600)
        st.plotly_chart(fig_amount)

    with col2:
        
        fig_count=px.bar(tran_amt_cnt_y_grp, x="States",y="Transaction_count",title=f" TRANSACTION COUNT--{tran_amt_cnt_yr["Years"].min()} QUARTER {quarter}",
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=650,width=600)
        st.plotly_chart(fig_count)
    
    col1,col2=st.columns(2)
    with col1:
        
        url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        response=requests.get(url)
        data1=json.loads(response.content)
        states_name=[]
        for data in data1["features"]:
            states_name.append(data["properties"]["ST_NM"])
            
        states_name.sort()

        fig_india_1=px.choropleth(tran_amt_cnt_y_grp,geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_amount",color_continuous_scale="thermal",
                                range_color=(tran_amt_cnt_y_grp["Transaction_amount"].min(),tran_amt_cnt_y_grp["Transaction_amount"].max()),
                                hover_name="States", title=f"TRANSACTION AMOUNT--{tran_amt_cnt_yr["Years"].min()} QUARTER {quarter} ",fitbounds="locations",
                                height=700,width=650)
        
        fig_india_1.update_geos(visible=False)
        st.plotly_chart(fig_india_1)
    
    with col2:    
        fig_india_2=px.choropleth(tran_amt_cnt_y_grp,geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_count",color_continuous_scale="thermal",
                                range_color=(tran_amt_cnt_y_grp["Transaction_count"].min(),tran_amt_cnt_y_grp["Transaction_count"].max()),
                                hover_name="States", title=f"TRANSACTION COUNT--{tran_amt_cnt_yr["Years"].min()} QUARTER {quarter} ",fitbounds="locations",
                                height=700,width=650)
        
        fig_india_2.update_geos(visible=False)
        st.plotly_chart(fig_india_2)
    
    return tran_amt_cnt_yr

def agg_tran_type(df,state):


    tran_amt_cnt_yr=df[df["States"]==state]
    tran_amt_cnt_yr.reset_index(drop=True,inplace=True)


    tran_amt_cnt_y_grp=tran_amt_cnt_yr.groupby("Transaction_type")[["Transaction_count","Transaction_amount"]].sum()
    tran_amt_cnt_y_grp.reset_index(inplace=True)

    col1,col2=st.columns(2)
    with col1:
        
        fig_pie_1=px.pie(data_frame=tran_amt_cnt_y_grp,names="Transaction_type", values="Transaction_amount",
                            width=600,title=f"TRANSACTION AMOUNT--{state.upper()}" )
        st.plotly_chart(fig_pie_1)
    with col2:
        
        fig_pie_2=px.pie(data_frame=tran_amt_cnt_y_grp,names="Transaction_type", values="Transaction_count",
                            width=600,title=f"TRANSACTION COUNT--{state.upper()}" )
        st.plotly_chart(fig_pie_2)


def agg_user_plot_1(df,year):   #agg user year analysis
    ag_user_year=df[df["Years"]==year]
    ag_user_year.reset_index(drop=True,inplace=True)
    ag_user_year_grp=pd.DataFrame(ag_user_year.groupby("Brands")["Transaction_count"].sum())
    ag_user_year_grp.reset_index(inplace=True)

    fig_bar_1=px.bar(ag_user_year_grp,x="Brands",y="Transaction_count",title=f"BRANDS AND TRANSACTION COUNT--{year}",
                    width=1000,color_discrete_sequence=px.colors.sequential.amp_r,hover_name="Brands")
    st.plotly_chart(fig_bar_1)
    
    return ag_user_year

#agg user analysis quarter 
def agg_user_plot_2(df,quarter):
    ag_user_year_Q=df[df["Quarter"]==quarter]
    ag_user_year_Q.reset_index(drop=True,inplace=True)
    ag_user_year_Q_Grp=pd.DataFrame(ag_user_year_Q.groupby("Brands")["Transaction_count"].sum())
    ag_user_year_Q_Grp.reset_index(inplace=True)

    fig_bar_1=px.bar(ag_user_year_Q_Grp,x="Brands",y="Transaction_count",title=f"BRANDS AND TRANSACTION COUNT QUARTER--{quarter}",
                    width=1000,color_discrete_sequence=px.colors.sequential.Bluered,hover_name="Brands")
    st.plotly_chart(fig_bar_1)
    
    return ag_user_year_Q

#aggregated user analysis for state
def agg_user_plot_3(df,state):
    Ag_user_year_Q_State=df[df["States"]==state]
    Ag_user_year_Q_State.reset_index(drop=True,inplace=True)
    fig_line_1=px.line(Ag_user_year_Q_State,x="Brands",y="Transaction_count",hover_data="Percentage",
                    title=f"AGGREGATED USER DATA OF BRANDS, TRANSACTION COUNT,PERCENTAGE--{state.upper()}",width=1000,markers=True)
    st.plotly_chart(fig_line_1)
    

#map insurance district details
def map_ins_district(df,state):


    tran_amt_cnt_yr=df[df["States"]==state]
    tran_amt_cnt_yr.reset_index(drop=True,inplace=True)


    tran_amt_cnt_y_grp=tran_amt_cnt_yr.groupby("District")[["Transaction_count","Transaction_amount"]].sum()
    tran_amt_cnt_y_grp.reset_index(inplace=True)
    col1,col2=st.columns(2)
    with col1:
        fig_bar_1=px.bar(tran_amt_cnt_y_grp,y="District", x="Transaction_amount",height=600,
                            orientation="h",title=f"DISTRICT TRANSACTION AMOUNT--{state.upper()}",color_discrete_sequence=px.colors.sequential.Bluered_r)
        st.plotly_chart(fig_bar_1)

    with col2:
        fig_bar_2=px.bar(tran_amt_cnt_y_grp,y="District", x="Transaction_count",height=600,
                            orientation="h",title=f"DISTRICT TRANSACTION COUNT--{state.upper()}",color_discrete_sequence=px.colors.sequential.Mint_r )
        st.plotly_chart(fig_bar_2)

#map user plot 1 details(year)

def map_user_plot_1(df,year):
    map_user_year=df[df["Years"]==year]
    map_user_year.reset_index(drop=True,inplace=True)
    map_user_year_grp=map_user_year.groupby("States")[["RegisteredUsers","AppOpens"]].sum()
    map_user_year_grp.reset_index(inplace=True)
    fig_line_1=px.line(map_user_year_grp,x="States",y=["RegisteredUsers","AppOpens"],
                        height=800,width=1000,title=f"REGISTERED USERS AND APPOPENS DATA--{year}",markers=True)
    st.plotly_chart(fig_line_1)
    
    return map_user_year

#map user plot 2 details(quarter)

def map_user_plot_2(df,quarter):
    map_user_year_quarter=df[df["Quarter"]==quarter]
    map_user_year_quarter.reset_index(drop=True,inplace=True)
    map_user_year_quarter_grp=map_user_year_quarter.groupby("States")[["RegisteredUsers","AppOpens"]].sum()
    map_user_year_quarter_grp.reset_index(inplace=True)
    fig_line_1=px.line(map_user_year_quarter_grp,x="States",y=["RegisteredUsers","AppOpens"],
                        height=800,width=1000,title=f"REGISTERED USERS AND APPOPENS DATA QUARTER--{quarter}",markers=True,
                        color_discrete_sequence=px.colors.sequential.Rainbow)
    st.plotly_chart(fig_line_1)
    return map_user_year_quarter
    
    
#map user plt 3 for reg user and appopens
def map_user_plot_3(df,states):
    map_user_year_quarter_state=df[df["States"]==states]
    map_user_year_quarter_state.reset_index(drop=True,inplace=True)

    col1,col2=st.columns(2)
    with col1:
        fig_map_user_bar_1=px.bar(map_user_year_quarter_state,x="RegisteredUsers",y="District",orientation="h",
                                title="REGISTERED USERS",height=800,color_discrete_sequence=px.colors.sequential.Rainbow)
        st.plotly_chart(fig_map_user_bar_1)
    with col2:
        fig_map_user_bar_2=px.bar(map_user_year_quarter_state,x="AppOpens",y="District",orientation="h",
                                title="APPOPENS",height=800,color_discrete_sequence=px.colors.sequential.Rainbow_r)
        st.plotly_chart(fig_map_user_bar_2)
        
    #top trans for pincodes
def top_trans_plot_1(df,state):
    top_trans_year=df[df["States"]==state]
    top_trans_year.reset_index(drop=True,inplace=True)

    col1,col2=st.columns(2)
    with col1:
        fig_top_trans_bar_1=px.bar(top_trans_year,x="Quarter",y="Transaction_amount",hover_data="Pincodes",
                                title="TRANSACTION AMOUNT FOR TOP TRANSACTION",height=650,width=600,color_discrete_sequence=px.colors.sequential.Electric)
        st.plotly_chart(fig_top_trans_bar_1)
    with col2:    
        fig_top_trans_bar_2=px.bar(top_trans_year,x="Quarter",y="Transaction_count",hover_data="Pincodes",
                                title="TRANSACTION COUNT FOR TOP TRANSACTION",height=650,width=600,color_discrete_sequence=px.colors.sequential.Agsunset_r)
        st.plotly_chart(fig_top_trans_bar_2)
        
#top user for year
def top_user_plot_1(df,year):
    top_user_year=df[df["Years"]==year]
    top_user_year.reset_index(drop=True,inplace=True)

    top_user_year_grp=pd.DataFrame(top_user_year.groupby(["States","Quarter"])["RegisteredUsers"].sum())
    top_user_year_grp.reset_index(inplace=True)
   

    fig_top_plot_1=px.bar(top_user_year_grp,x="States",y="RegisteredUsers",color="Quarter",width=1000,hover_name="States",
                        title=f"TOP USER REGISTERED USERS--{year}",height=800,color_discrete_sequence=px.colors.sequential.Burgyl)
    st.plotly_chart(fig_top_plot_1)
    
    return top_user_year 

#top user plot for state
def top_user_plot_2(df,state):
    top_user_year_state=df[df["States"]==state]
    top_user_year_state.reset_index(drop=True,inplace=True)
    fig_top_plot_2=px.bar(top_user_year_state,x="Quarter",y="RegisteredUsers",title="REGISTERED USERS AND PINCODES FOR EACH QUARTER",
                        
                        width=1000,height=800,color="RegisteredUsers",hover_data="Pincodes",color_continuous_scale=px.colors.sequential.Bluered_r)
    st.plotly_chart(fig_top_plot_2)  

#fig for top mob brands    
def top_fig_agg_user_brand():
    brand= Agg_user[["Brands","Transaction_count"]]
    brand1= brand.groupby("Brands")["Transaction_count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "Transaction_count", names= "Brands", color_discrete_sequence=px.colors.sequential.dense_r,
                       title= "Top Mobile Brands of Transaction_count")
    st.plotly_chart(fig_brands)

#fig for lower transaction    
def top_fig_map_trans():
    dt= Map_transaction[["District", "Transaction_amount"]]
    dt1= dt.groupby("District")["Transaction_amount"].sum().sort_values(ascending=True)
    dt2= pd.DataFrame(dt1).reset_index().head(50)

    fig_dis_map_trans= px.bar(dt2, x= "District", y= "Transaction_amount", title= "DISTRICTS WITH LOWEST TRANSACTION AMOUNT",
                color_discrete_sequence= px.colors.sequential.Mint_r,height=800,width=600)
    st.plotly_chart(fig_dis_map_trans)

#figure for transaction amount 
def top_fig_trans_amount(table_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        port="5432",
                        database="Phonepe",
                        password="12345")
    cursor=mydb.cursor()

    #plot for descending order
    query_1=f'''SELECT states,SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount DESC
                LIMIT 10;'''
    cursor.execute(query_1)
    table1=cursor.fetchall()
    mydb.commit()
    df_1=pd.DataFrame(table1,columns=("states","transaction_amount"))
    col1,col2=st.columns(2)
    with col1:
        fig_amount_1=px.bar(df_1, x="states",y="transaction_amount",title="TOP 10 TRANSACTION AMOUNT",hover_name="states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r,height=650,width=600)
        st.plotly_chart(fig_amount_1)

    #plot for acsending order
    query_2=f'''SELECT states,SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount 
                LIMIT 10;'''
    cursor.execute(query_2)
    table2=cursor.fetchall()
    mydb.commit()
    df_2=pd.DataFrame(table2,columns=("states","transaction_amount"))
    with col2:
        fig_amount_2=px.bar(df_2, x="states",y="transaction_amount",title="LAST 10 TRANSACTION AMOUNT",hover_name="states",
                            color_discrete_sequence=px.colors.sequential.Bluered,height=650,width=600)
        st.plotly_chart(fig_amount_2)

    #plot for average value

    query_3=f'''SELECT states,AVG(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount;'''
    cursor.execute(query_3)
    table3=cursor.fetchall()
    mydb.commit()
    df_3=pd.DataFrame(table3,columns=("states","transaction_amount"))
    fig_amount_3=px.bar(df_3, x="transaction_amount",y="states",title="AVERAGE TRANSACTION AMOUNT",hover_name="states",orientation="h",
                        color_discrete_sequence=px.colors.sequential.Cividis_r,height=800,width=1000)
    st.plotly_chart(fig_amount_3)
   

def top_fig_trans_count(table_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        port="5432",
                        database="Phonepe",
                        password="12345")
    cursor=mydb.cursor()

    #plot for descending order
    query_1=f'''SELECT states,SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count DESC
                LIMIT 10;'''
    cursor.execute(query_1)
    table1=cursor.fetchall()
    mydb.commit()
    df_1=pd.DataFrame(table1,columns=("states","transaction_count"))
    col1,col2=st.columns(2)
    with col1:
        fig_amount_1=px.bar(df_1, x="states",y="transaction_count",title="TOP 10 TRANSACTION COUNT",hover_name="states",
                            color_discrete_sequence=px.colors.sequential.GnBu_r,height=650,width=600)
        st.plotly_chart(fig_amount_1)

    #plot for acsending order
    query_2=f'''SELECT states,SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count
                LIMIT 10;'''
    cursor.execute(query_2)
    table2=cursor.fetchall()
    mydb.commit()
    df_2=pd.DataFrame(table2,columns=("states","transaction_count"))
    with col2:
        fig_amount_2=px.bar(df_2, x="states",y="transaction_count",title="LAST 10 TRANSACTION COUNT",hover_name="states",
                            color_discrete_sequence=px.colors.sequential.Darkmint_r,height=650,width=600)
        st.plotly_chart(fig_amount_2)

    #plot for average value

    query_3=f'''SELECT states,AVG(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count;'''
    cursor.execute(query_3)
    table3=cursor.fetchall()
    mydb.commit()
    df_3=pd.DataFrame(table3,columns=("states","transaction_count"))
    fig_amount_3=px.bar(df_3, x="transaction_count",y="states",title="AVERAGE TRANSACTION COUNT",hover_name="states",orientation="h",
                        color_discrete_sequence=px.colors.sequential.Blackbody,height=850,width=1000)
    st.plotly_chart(fig_amount_3)\



def top_fig_registered_user(table_name,state):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        port="5432",
                        database="Phonepe",
                        password="12345")
    cursor=mydb.cursor()

    #plot for descending order
    query_1=f'''SELECT districts,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registeredusers DESC
                LIMIT 10;'''
    cursor.execute(query_1)
    table1=cursor.fetchall()
    mydb.commit()
    df_1=pd.DataFrame(table1,columns=("districts","registeredusers"))
    col1,col2=st.columns(2)
    with col1:
    
        fig_amount_1=px.bar(df_1, x="districts",y="registeredusers",title="TOP 10 REGISTERED USERS",hover_name="districts",
                            color_discrete_sequence=px.colors.sequential.GnBu_r,height=650,width=600)
        st.plotly_chart(fig_amount_1)

    #plot for acsending order
    query_2=f'''SELECT districts,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registeredusers 
                LIMIT 10;'''
    cursor.execute(query_2)
    table2=cursor.fetchall()
    mydb.commit()
    df_2=pd.DataFrame(table2,columns=("districts","registeredusers"))
    with col2:
        fig_amount_2=px.bar(df_2, x="districts",y="registeredusers",title="LAST 10 REGISTERED USERS",hover_name="districts",
                            color_discrete_sequence=px.colors.sequential.Rainbow,height=650,width=600)
        st.plotly_chart(fig_amount_2)

    #plot for average value

    query_3=f'''SELECT districts,AVG(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states='{state}'
                GROUP BY districts
                ORDER BY registeredusers;'''
    cursor.execute(query_3)
    table3=cursor.fetchall()
    mydb.commit()
    df_3=pd.DataFrame(table3,columns=("districts","registeredusers"))
    fig_amount_3=px.bar(df_3, x="registeredusers",y="districts",title="AVERAGE REGISTERED USERS",hover_name="registeredusers",orientation="h",
                        color_discrete_sequence=px.colors.sequential.Cividis_r,height=800,width=1000)
    st.plotly_chart(fig_amount_3)



def top_fig_app_opens(table_name,state):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        port="5432",
                        database="Phonepe",
                        password="12345")
    cursor=mydb.cursor()

    #plot for descending order
    query_1=f'''SELECT districts,SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens DESC
                LIMIT 10;'''
    cursor.execute(query_1)
    table1=cursor.fetchall()
    mydb.commit()
    df_1=pd.DataFrame(table1,columns=("districts","appopens"))
    col1,col2=st.columns(2)
    with col1:
        fig_amount_1=px.bar(df_1, x="districts",y="appopens",title="TOP 10 APPOPENS",hover_name="districts",
                            color_discrete_sequence=px.colors.sequential.GnBu_r,height=650,width=600)
        st.plotly_chart(fig_amount_1)

    #plot for acsending order
    query_2=f'''SELECT districts,SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens 
                LIMIT 10;'''
    cursor.execute(query_2)
    table2=cursor.fetchall()
    mydb.commit()
    df_2=pd.DataFrame(table2,columns=("districts","appopens"))
    with col2:
        fig_amount_2=px.bar(df_2, x="districts",y="appopens",title="LAST 10 APPOPENS",hover_name="districts",
                            color_discrete_sequence=px.colors.sequential.Rainbow,height=650,width=600)
        st.plotly_chart(fig_amount_2)

    #plot for average value

    query_3=f'''SELECT districts,AVG(appopens) AS appopens
                FROM {table_name}
                WHERE states='{state}'
                GROUP BY districts
                ORDER BY appopens;'''
    cursor.execute(query_3)
    table3=cursor.fetchall()
    mydb.commit()
    df_3=pd.DataFrame(table3,columns=("districts","appopens"))
    fig_amount_3=px.bar(df_3, x="appopens",y="districts",title="AVERAGE APPOPENS",hover_name="appopens",orientation="h",
                        color_discrete_sequence=px.colors.sequential.Cividis_r,height=800,width=1000)
    st.plotly_chart(fig_amount_3)



def top_fig_registered_users(table_name):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        port="5432",
                        database="Phonepe",
                        password="12345")
    cursor=mydb.cursor()

    #plot for descending order
    query_1=f'''SELECT states,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers DESC
                LIMIT 10;'''
    cursor.execute(query_1)
    table1=cursor.fetchall()
    mydb.commit()
    df_1=pd.DataFrame(table1,columns=("States","registeredusers"))
    col1,col2=st.columns(2)
    with col1:
        fig_amount_1=px.bar(df_1, x="States",y="registeredusers",title="TOP 10 REGISTERED USERS",hover_name="States",
                            color_discrete_sequence=px.colors.sequential.Pinkyl,height=650,width=600)
        st.plotly_chart(fig_amount_1)

    #plot for acsending order
    query_2=f'''SELECT states,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers 
                LIMIT 10;'''
    cursor.execute(query_2)
    table2=cursor.fetchall()
    mydb.commit()
    df_2=pd.DataFrame(table2,columns=("States","registeredusers"))
    
    with col2:
        fig_amount_2=px.bar(df_2, x="States",y="registeredusers",title="LAST 10 REGISTERED USERS",hover_name="States",
                            color_discrete_sequence=px.colors.sequential.solar_r,height=650,width=600)
        st.plotly_chart(fig_amount_2)

    #plot for average value

    query_3=f'''SELECT states,AVG(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers;'''
    cursor.execute(query_3)
    table3=cursor.fetchall()
    mydb.commit()
    df_3=pd.DataFrame(table3,columns=("States","registeredusers"))
    fig_amount_3=px.bar(df_3, x="registeredusers",y="States",title="AVERAGE REGISTERED USERS",orientation="h",
                        color_discrete_sequence=px.colors.sequential.Cividis_r,height=800,width=1000)
    st.plotly_chart(fig_amount_3)

        
#streamlit part for the phonepe

st.set_page_config(page_title="Phonepe pulse visualisation",layout="wide")
st.title("Phonepe Pulse Data Visualization and Exploration Using Streamlit and Plotly")
st.markdown("_This project involes in extracting the large amount of data from the Github repository which contains large amount of data related to various metrics and statistics. The main goal is to process the extracted information,form proper insight and visualise in the userfriendly manner._")

with st.sidebar:
    select=option_menu("Main Content",["Home","Data Analysis","Top figures"])
if select=="Home":
    col1,col2=st.columns(2)
    with col1:
        img=Image.open(r"C:\Users\Admin\Downloads\download.jpg")
        st.image(img,width=500)
    with col2:
        mygif = base64.b64encode(open(r"C:\Users\Admin\Downloads\dfdd76_d5164cf9221d41b6bb7142049d4f82a5~mv2.gif", 'rb').read()).decode()
        st.markdown(f"""<img src="data:png;base64,{mygif}" width='600' height='370' >""", True)
    
    st.video(r"C:\Users\Admin\Downloads\You can quickly pay with no PIN with UPI lite on PhonePe.mp4")
    col5,col6=st.columns(2)
    with col5:
        mygif1 = base64.b64encode(open(r"C:\Users\Admin\Downloads\main-qimg-740245eabf7a6dabceabc2356be1d5f4.gif", 'rb').read()).decode()
        st.markdown(f"""<img src="data:png;base64,{mygif1}" width='600' height='370' >""", True)
    with col6:
        st.video(r"C:\Users\Admin\Downloads\PhonePe Motion Graphics.mp4")
    
    st.link_button(":red[DOWNLOAD THE APP NOW]", "https://www.phonepe.com/app-download/")
        
elif select=="Data Analysis":
    tab1,tab2,tab3=st.tabs(["Aggregated Research","Map Research","Top Research"])
    
    with tab1:
        method=st.radio("Select the desired investigation",["Aggregated Transaction Survey","Aggregated User Survey"])
        
        
                
        if method=="Aggregated Transaction Survey":
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year",Agg_transaction["Years"].min(),Agg_transaction["Years"].max(),Agg_transaction["Years"].min())
            AT_tac_y=Transaction_amount_count_Y(Agg_transaction,years)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state",AT_tac_y["States"].unique())
            agg_tran_type(AT_tac_y,states)
            
            col1,col2=st.columns(2)
            with col1:
                quarters=st.slider("Select the desired Quarter",AT_tac_y["Quarter"].min(),AT_tac_y["Quarter"].max(),AT_tac_y["Quarter"].min())
            AT_tac_y_Q=Transaction_amount_count_Y_Q(AT_tac_y,quarters)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state_type",AT_tac_y_Q["States"].unique())
            agg_tran_type(AT_tac_y_Q,states)
            
        elif method=="Aggregated User Survey":
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year",Agg_user["Years"].min(),Agg_user["Years"].max(),Agg_user["Years"].min())
            Ag_user_year=agg_user_plot_1(Agg_user,years)
            
            col1,col2=st.columns(2)
            with col1:
                quarters=st.slider("Select the desired Quarter",Ag_user_year["Quarter"].min(),Ag_user_year["Quarter"].max(),Ag_user_year["Quarter"].min())
            Ag_user_year_Quarter=agg_user_plot_2(Ag_user_year,quarters)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state_type",Ag_user_year_Quarter["States"].unique())
            agg_user_plot_3(Ag_user_year_Quarter,states)
            
            
    with tab2:
        method2=st.radio("Select the desired investigation",["Map Transaction Survey","Map User Survey"])
        
        
            
        if method2=="Map Transaction Survey":
            
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year for map transaction",Map_transaction["Years"].min(),Map_transaction["Years"].max(),Map_transaction["Years"].min())
            map_trans_tac_year=Transaction_amount_count_Y(Map_transaction,years)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state for map transaction",map_trans_tac_year["States"].unique())
            map_ins_district(map_trans_tac_year,states)
            
            col1,col2=st.columns(2)
            with col1:
                quarters=st.slider("Select the desired Quarter_map transaction",map_trans_tac_year["Quarter"].min(),map_trans_tac_year["Quarter"].max(),map_trans_tac_year["Quarter"].min())
            map_trans_tac_y_Q=Transaction_amount_count_Y_Q(map_trans_tac_year,quarters)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state_type for district", map_trans_tac_y_Q["States"].unique())
            map_ins_district( map_trans_tac_y_Q,states)
        
        elif method2=="Map User Survey":
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year for map user",Map_user["Years"].min(),Map_user["Years"].max(),Map_user["Years"].min())
            map_user_year=map_user_plot_1(Map_user,years)

            col1,col2=st.columns(2)
            with col1:
                quarters=st.slider("Select the desired Quarter for map user", map_user_year["Quarter"].min(), map_user_year["Quarter"].max(), map_user_year["Quarter"].min())
            map_user_year_quarter=map_user_plot_2( map_user_year,quarters)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state_type of map user for district",map_user_year_quarter["States"].unique())
            map_user_plot_3( map_user_year_quarter,states)
            
    with tab3:
        method3=st.radio("Select the desired investigation",["Top Transaction Survey","Top User Survey"])
        
        if method3=="Top Transaction Survey":
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year for top transaction",Top_transaction["Years"].min(),Top_transaction["Years"].max(),Top_transaction["Years"].min())
            top_trans_tac_year=Transaction_amount_count_Y(Top_transaction,years)

            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state for top transaction",top_trans_tac_year["States"].unique())
            top_trans_plot_1(top_trans_tac_year,states)
            
            col1,col2=st.columns(2)
            with col1:
                quarters=st.slider("Select the desired Quarter for top transaction", top_trans_tac_year["Quarter"].min(), top_trans_tac_year["Quarter"].max(), top_trans_tac_year["Quarter"].min())
            top_trans_tac_year_quarter=Transaction_amount_count_Y_Q( top_trans_tac_year,quarters)
            
        elif method3=="Top User Survey":
            col1,col2=st.columns(2)
            with col1:
                years=st.slider("Select the desired year for top user",Top_user["Years"].min(),Top_user["Years"].max(),Top_user["Years"].min())
            top_user_year=top_user_plot_1(Top_user,years)
            
            col1,col2=st.columns(2)
            with col1:
                states=st.selectbox("Select the state for top user", top_user_year["States"].unique())
            top_user_plot_2( top_user_year,states)
            
        
elif select=="Top figures":
    
    ques=st.selectbox("Select your question",["1.Specify the top brands of mobiles used in aggregated user",
                                              "2.List down the Transaction amount  for aggregated transaction",
                                              "3.Top 50 Districts With Lowest Transaction Amount",
                                               "4.List down the Transaction count  for aggregated transaction",
                                               "5.List the transaction count for the aggregated user",
                                               "6. What is the registered users for the map user",
                                               "7. Organize the Transaction amount  for the top transaction",
                                               "8. Organize the Transaction count  for the top transaction",
                                               "9. specify the app opens for the map user",
                                                "10.Specify the Transaction amount  for the map transaction",
                                                "11. Organize the registered users for the top user",
                                                "12.Specify the Transaction count  for the map transaction"
                                                ])  
    if ques=="1.Specify the top brands of mobiles used in aggregated user":
        st.subheader("TOP MOBILE BRANDS IN AGGREGATED USER")
        top_fig_agg_user_brand()
    
    if ques=="2.List down the Transaction amount  for aggregated transaction":
        st.subheader("TRANSACTION AMOUNT")
        top_fig_trans_amount("agg_transaction")
    
    if ques=="3.Top 50 Districts With Lowest Transaction Amount":
        st.subheader("TOP 50 DISTRICTS WITH LOW TRANSACTION AMOUNT")
        top_fig_map_trans()
        
        
    if ques=="4.List down the Transaction count  for aggregated transaction":
        st.subheader("TRANSACTION COUNT") 
        top_fig_trans_count("agg_transaction")   
        
    elif ques=="5.List the transaction count for the aggregated user":
        
        st.subheader("TRANSACTION COUNT") 
        top_fig_trans_count("agg_user")   
    
    elif ques=="6. What is the registered users for the map user":
        states=st.selectbox("Select the appropriate state",Map_user["States"].unique())
        st.subheader("REGISTERED USERS") 
        top_fig_registered_user("map_user",states)   
    
    elif ques=="7. Organize the Transaction amount  for the top transaction":
        st.subheader("TRANSACTION AMOUNT")
        top_fig_trans_amount("top_transaction")  
        
    elif ques=="8. Organize the Transaction count for the top transaction":  
        st.subheader("TRANSACTION COUNT") 
        top_fig_trans_count("top_transaction") 
        
    elif ques=="9. specify the app opens for the map user":
        states=st.selectbox("Select the appropriate state",Map_user["States"].unique())
        st.subheader("APP OPENS") 
        top_fig_app_opens("map_user",states)   
    
    elif ques=="10.Specify the Transaction amount  for the map transaction":
        st.subheader("TRANSACTION AMOUNT") 
        top_fig_trans_amount("map_transaction")   
        
    
    elif ques=="11. Organize the registered users for the top user":
        
        st.subheader("APP OPENS") 
        top_fig_registered_users("top_user")   
          
    elif ques=="12.Specify the Transaction count  for the map transaction":    
        st.subheader("TRANSACTION COUNT")
        top_fig_trans_count("map_transaction")  
        
    
   
    
    