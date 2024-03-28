import pymongo
import pandas as pd
pd.set_option("display.max_columns",None)
import streamlit as st
from streamlit_option_menu import option_menu
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import base64



# Streamlit part

st.set_page_config(layout= "wide")
st.title("AIRBNB DATA ANALYSIS")


st.write("")

def datafr():
    df1=pd.read_csv(r"C:\Users\Viswa.DP-PC\New folder\project\airbnb\Airbnb.csv")
    return df1

df1=datafr()

with st.sidebar:
    select= option_menu("Main Menu", ["Home", "Data Exploration", "Techniques used"])

if select == "Home":
    st.video(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\Close Friends (1).mp4")
    st.write("")
    st.write("")
    st.write("")
    col1,col2,col3=st.columns(3)
    with col3:
        st.link_button(":black[BOOK NOW!!!]", "https://www.airbnb.co.in/")

    with col1:
        mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\airbnb.gif", 'rb').read()).decode()
        st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)

if select == "Data Exploration":
    if select == "Data Exploration":
        tab1, tab2, tab3, tab4, tab5= st.tabs(["***PRICE ANALYSIS***","***AVAILABILITY ANALYSIS***","***LOCATION BASED***", "***GEOSPATIAL VISUALIZATION***", "***TOP CHARTS***"])
    with tab1:
        
        st.title("**PRICE DIFFERENCE**")
        col1,col2=st.columns(2)

        with col1:
            
            country=st.selectbox("**Select The Country**",df1["country"].unique())

            
           
   
            
            df2=df1[df1["country"]==country]
            df2.reset_index(drop=True,inplace=True)

            room_type=st.selectbox("**Select The Room Type**",df2["room_type"].unique())

            df3= df2[df2["room_type"] == room_type]
            df3.reset_index(drop= True, inplace= True)

            df_bar= pd.DataFrame(df3.groupby("property_type")[["price","review_scores","number_of_reviews","country"]].sum())
            df_bar.reset_index(inplace= True)

            fig_bar= px.bar(df_bar, x='property_type', y= "price", title= "***PRICE FOR PROPERTY_TYPES***",
            hover_data=["number_of_reviews","review_scores","country"],color="country", 
            width=600, height=500)
            st.plotly_chart(fig_bar)


        with col2:
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            property_type=st.selectbox("**Select The Property Type**",df3["property_type"].unique())

            df4=df3[df3["property_type"]==property_type]
            df4.reset_index(drop=True,inplace=True)

            df_pie= pd.DataFrame(df4.groupby("host_response_time")[["price","bedrooms"]].sum())
            df_pie.reset_index(inplace= True)

            fig_pi= px.pie(df_pie, values="price", names= "host_response_time",
                            hover_data=["bedrooms"],
                            color_discrete_sequence=px.colors.sequential.BuPu_r,
                            title="***PRICE DIFFERENCE BASED ON HOST RESPONSE TIME***",
                            width= 600, height= 500)
            st.plotly_chart(fig_pi)

        col1,col2=st.columns(2)

        with col1:
            hostresponsetime=st.selectbox("**Select The Host_Response_Time**",df4["host_response_time"].unique())

            df5=df4[df4["host_response_time"]==hostresponsetime]


            df_bar1= pd.DataFrame(df5.groupby("bed_type")[["minimum_nights","maximum_nights","price"]].sum())
            df_bar1.reset_index(inplace= True)

            fig_bar1 = px.bar(df_bar1, x='bed_type', y=['minimum_nights', 'maximum_nights'], 
            title='***MINIMUM NIGHTS AND MAXIMUM NIGHTS***',hover_data="price",
            barmode='group',color_discrete_sequence=px.colors.sequential.Rainbow, width=600, height=500)
            

            st.plotly_chart(fig_bar1)


        with col2:
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            df_bar2=pd.DataFrame(df5.groupby("bed_type")[["bedrooms","beds","accommodates","price"]].sum())
            df_bar2.reset_index(inplace=True)


            fig_bar2=px.bar(df_bar2,x="bed_type",y=["bedrooms","beds","accommodates"],title="***BEDROOM AND BEDS ACCOMMODATES***",
            hover_data="price",barmode='group',color_discrete_sequence=px.colors.sequential.Hot_r,width=600,height=550)

            st.plotly_chart(fig_bar2)


    with tab2:
        st.title("***AVAILABILITY ANALYSIS***")

        def datafr():
            df6=pd.read_csv(r"C:\Users\Viswa.DP-PC\New folder\project\airbnb\Airbnb.csv")
            return df6

        df6=datafr()

        col1,col2=st.columns(2)

        with col1:
            country_a=st.selectbox("**Select The Country_A**",df6["country"].unique())

            df7=df1[df1["country"]==country_a]
            df7.reset_index(drop=True,inplace=True)


            property_type_a=st.selectbox("**Select The Property Type_P**",df7["property_type"].unique())

            df8=df7[df7["property_type"]==property_type_a]
            df8.reset_index(drop=True,inplace=True)

            df8_sunburst=px.sunburst(df8,path=["room_type","bed_type","is_location_exact"], values="availability_30",
            width=600,height=500,title="***Availability_30***",color_discrete_sequence=px.colors.sequential.Peach_r)

            st.plotly_chart(df8_sunburst)


        with col2:
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")
            st.write("")

            df8_sunburst_60=px.sunburst(df8,path=["room_type","bed_type","is_location_exact"], values="availability_60",
            width=600,height=500,title="***Availability_60***",color_discrete_sequence=px.colors.sequential.Agsunset_r)

            st.plotly_chart(df8_sunburst_60)


        col1,col2=st.columns(2)

        with col1:
            df8_sunburst_90=px.sunburst(df8,path=["room_type","bed_type","is_location_exact"], values="availability_90",
            width=600,height=500,title="***Availability_90***",color_discrete_sequence=px.colors.sequential.Burgyl_r)

            st.plotly_chart(df8_sunburst_90)

        with col2:
            df8_sunburst_365=px.sunburst(df8,path=["room_type","bed_type","is_location_exact"], values="availability_365",
            width=600,height=500,title="***Availability_365***",color_discrete_sequence=px.colors.sequential.Oranges_r)

            st.plotly_chart(df8_sunburst_365)


        room_type_a=st.selectbox("**Select The Room_Type_a**",df8["room_type"].unique())

        df9=df8[df8["room_type"] == room_type_a]
        df_bar3=pd.DataFrame(df9.groupby("host_response_time")[["availability_30","availability_60","availability_90","price"]].sum())
        df_bar3.reset_index(inplace=True)

        fig_bar3=px.bar(df_bar3,x="host_response_time",y=["availability_30","availability_60","availability_90"],hover_data="price",
                        title="***AVAILABILITY BASED ON HOST RESPONSE TIME***",
                        width=1000,barmode="group",color_discrete_sequence=px.colors.sequential.Hot)


        st.plotly_chart(fig_bar3)
    
    with tab3:
        st.write("LOCATION ANALYSIS")
        st.write("")

        def datafr():
            df=pd.read_csv(r"C:\Users\Viswa.DP-PC\New folder\project\airbnb\Airbnb.csv")
            return df

        df_l=datafr()

        country_l=st.selectbox("**Select The Country_l**",df_l["country"].unique())

        df1_l=df_l[df_l["country"] == country_l]
        df1_l.reset_index(drop=True,inplace=True)

        property_type_l=st.selectbox("**Select The Property Type_l**",df1_l["property_type"].unique())

        df2_l=df1_l[df1_l["property_type"]==property_type_l]
        df2_l.reset_index(drop=True,inplace=True)

        st.write(" ")

        def select_the_df(sel_val):
            if sel_val == str(df2_l['price'].min())+' '+str('to')+' '+str(differ_max_min*0.30 + df2_l['price'].min())+' '+str("(30% of the Value)"):

                df_val_30= df2_l[df2_l["price"] <= differ_max_min*0.30 + df2_l['price'].min()]
                df_val_30.reset_index(drop= True, inplace= True)
                return df_val_30


            elif sel_val == str(differ_max_min*0.30 + df2_l['price'].min())+' '+str('to')+' '+str(differ_max_min*0.60 + df2_l['price'].min())+' '+str("(30% to 60% of the Value)"):
        
                df_val_60= df2_l[df2_l["price"] >= differ_max_min*0.30 + df2_l['price'].min()]
                df_val_60_1= df_val_60[df_val_60["price"] <= differ_max_min*0.60 + df2_l['price'].min()]
                df_val_60_1.reset_index(drop= True, inplace= True)
                return df_val_60_1
            
            elif sel_val == str(differ_max_min*0.60 + df2_l['price'].min())+' '+str('to')+' '+str(df2_l['price'].max())+' '+str("(60% to 100% of the Value)"):

                df_val_100= df2_l[df2_l["price"] >= differ_max_min*0.60 + df2_l['price'].min()]
                df_val_100.reset_index(drop= True, inplace= True)
                return df_val_100
            
        differ_max_min= df2_l['price'].max()-df2_l['price'].min()

        val_sel= st.radio("**Select the Price Range**",[str(df2_l['price'].min())+' '+str('to')+' '+str(differ_max_min*0.30 + df2_l['price'].min())+' '+str("(30% of the Value)"),
                                                    
                                                    str(differ_max_min*0.30 + df2_l['price'].min())+' '+str('to')+' '+str(differ_max_min*0.60 + df2_l['price'].min())+' '+str("(30% to 60% of the Value)"),

                                                    str(differ_max_min*0.60 + df2_l['price'].min())+' '+str('to')+' '+str(df2_l['price'].max())+' '+str("(60% to 100% of the Value)")])
                                          
        df_val_sel= select_the_df(val_sel)

        st.dataframe(df_val_sel)

        # checking the correlation

        df_val_sel_corr= df_val_sel.drop(columns=["listing_url","name","description","transit","house_rules", "property_type",                 
                                            "room_type", "bed_type","cancellation_policy",
                                            "images","host_url","host_name", "host_location",                   
                                            "host_response_time", "host_thumbnail_url",            
                                            "host_response_rate","host_is_superhost","host_has_profile_pic" ,         
                                            "host_picture_url","host_neighbourhood",
                                            "host_identity_verified","host_verifications",
                                            "street", "suburb", "government_area", "market",                        
                                            "country", "country_code","location_type","is_location_exact",
                                            "amenities"]).corr()
        
        st.dataframe(df_val_sel_corr)

        df_val_sel_gr= pd.DataFrame(df_val_sel.groupby("accommodates")[["cleaning_fee","bedrooms","beds","extra_people","country"]].sum())
        df_val_sel_gr.reset_index(inplace= True)

        fig_1= px.bar(df_val_sel_gr, x="accommodates", y= ["cleaning_fee","bedrooms","beds"], title="***ACCOMMODATES***",
                    hover_data= "extra_people", barmode='group', hover_name="country",
                    color_discrete_sequence=px.colors.sequential.Rainbow_r,width=1000)
        st.plotly_chart(fig_1)

        
        room_ty_l= st.selectbox("**Select The Room_Type_l**", df_val_sel["room_type"].unique())

        df_val_sel_rt= df_val_sel[df_val_sel["room_type"] == room_ty_l]

        fig_2= px.bar(df_val_sel_rt, x= ["street","host_location","host_neighbourhood"],y="market", title="**MARKET**",
                    hover_data= ["name","host_name","market"], barmode='group',orientation='h', color_discrete_sequence=px.colors.sequential.Rainbow_r,width=1000)
        st.plotly_chart(fig_2)

        

    with tab4:

        def datafr():
            df=pd.read_csv(r"C:\Users\Viswa.DP-PC\New folder\project\airbnb\Airbnb.csv")
            return df

        st.title("GEOSPATIAL VISUALIZATION")
        st.write("")

        fig_4 = px.scatter_mapbox(df1,lat='latitude', lon='longitude', color='price', size='review_scores',
                        color_continuous_scale='aggrnyl',hover_name='name',range_color=(0,49000),hover_data="accommodates", 
                        mapbox_style="carto-positron",zoom=1)
        fig_4.update_layout(width=1150,height=800,title='***GEOSPATIAL DISTRIBUTION OF LISTINGS***')
        st.plotly_chart(fig_4) 


    with tab5:
        country_t=st.selectbox("**Select The Country_t**",df1["country"].unique())
        df11=df1[df1["country"]== country_t]

        property_type_t=st.selectbox("**Select The Property Type_t**",df11["property_type"].unique())

        df12=df11[df11["property_type"] == property_type_t]
        df12.reset_index(drop=True,inplace=True)

        df12_sorted=df12.sort_values(by='price')
        df12_sorted.reset_index(drop=True,inplace=True)

        df_price= pd.DataFrame(df12_sorted.groupby("host_neighbourhood")["price"].agg(["sum","mean"]))
        df_price.reset_index(inplace= True)
        df_price.columns= ["host_neighbourhood", "Total_price", "Average_price"]

        col1,col2=st.columns(2)

        with col1:
            fig_price= px.bar(df_price, x= "Total_price", y= "host_neighbourhood", orientation='h',
                            title= "***PRICE BASED ON HOST_NEIGHBOURHOOD***", width= 600, height= 800,
                            color_discrete_sequence=px.colors.sequential.Burgyl_r)
            st.plotly_chart(fig_price)


        with col2:
            fig_price1= px.bar(df_price, x= "Average_price", y= "host_neighbourhood", orientation='h',
                            title= "***AVERAGE PRICE BASED ON HOST_NEIGHBOURHOOD***", width= 600, height= 800,
                            color_discrete_sequence=px.colors.sequential.BuPu)
            st.plotly_chart(fig_price1)

        col1,col2=st.columns(2)
        with col1:
            df_price1=pd.DataFrame(df12_sorted).groupby("host_location")["price"].agg(["sum","mean"])
            df_price1.reset_index(inplace=True)
            df_price1.columns=["host_location","Total_price","Average_price"]

            fig_price2=px.line(df_price1,x="Total_price",y="host_location",title="***PRICE BASED ON HOST_LOCATION***",
                                color_discrete_sequence=px.colors.sequential.Aggrnyl,hover_name="host_location",markers=True)

            st.plotly_chart(fig_price2)

        with col2:
            fig_price3=px.line(df_price1,x="Average_price",y="host_location",title="***AVERAGE PRICE BASED ON HOST_LOCATION***",
                                color_discrete_sequence=px.colors.sequential.Blackbody,hover_name="host_location",markers="%")

            st.plotly_chart(fig_price3)


    room_type_t=st.selectbox("**Select The Room Type_t**",df12_sorted["room_type"].unique())
    df13=df12_sorted[df12_sorted["room_type"] == room_type_t]

    df13_sorted=df13.sort_values(by='price')
    df13_sorted.reset_index(drop=True,inplace=True)

    fig_price4=px.bar(df13_sorted,x="name",y="price",color="price", color_continuous_scale= "rainbow",
                                range_color=(0,df13_sorted["price"].max()),
                                title= "***MINIMUM_NIGHTS MAXIMUM_NIGHTS AND ACCOMMODATES***",
                                width=1200, height= 800,
                                hover_data= ["minimum_nights","maximum_nights","accommodates"],
                                hover_name="country")

    st.plotly_chart(fig_price4)

    fig_price5=px.bar(df13_sorted,x="name",y="price",color="price", color_continuous_scale= "reds",
                                range_color=(0,df13_sorted["price"].max()),
                                title= "***BEDROOMS BEDS BEDTYPES AND ACCOMMODATES***",
                                width=1200, height= 800,
                                hover_data= ["accommodates","bedrooms","beds","bed_type"],
                                hover_name="country")

    st.plotly_chart(fig_price5)
    
if select=="Techniques used":
    col1,col2=st.columns(2)
    with col1:
        with st.expander("STEP1"):
            st.write("Data Retrieval From MongoDB")
            mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\1628882237645.gif", 'rb').read()).decode()
            st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)
            mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\ded3fcfbf7d498e3fe4852426241b2ac.gif", 'rb').read()).decode()
            st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)


    with col2:
        with st.expander("STEP2"):
            st.write("Data Cleaning and Preprocessing")

            mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\1_mf619XEdHd1O2MlyhEKcig.gif", 'rb').read()).decode()
            st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)

    col1,col2=st.columns(2)
    with col2:
        with st.expander("DATA VISUALIZATION"):
            st.write("Data visualization is the graphical representation of data and information. ")
            img=(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\images.jpg")
            st.image(img)

            st.write("GEOSPATIAL VISUALIZATION")
            st.write("PRICE ANALYSIS AND VISUALIZATION")
            st.write("AVAILABILITY ANALYSIS")
            st.write("LOCATION BASED IN SIGHTS")

            

    with col1:
        with st.expander("STEP3"):
            st.write("Streamlit Visualization")
            
            mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\streamlit_dashboard_matplotlib.gif", 'rb').read()).decode()
            st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)

    with st.expander("STEP4"):
        st.write("POWER BI DASHBOARD")
        mygif = base64.b64encode(open(r"C:\Users\Viswa.DP-PC\Desktop\airbnb\f617f080d4d78bdee1c6615397bebc6a.gif", 'rb').read()).decode()
        st.markdown(f"""<img src="data:png;base64,{mygif}" width='1000' height='500' >""", True)

            
    










            