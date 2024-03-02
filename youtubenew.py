import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import sqlalchemy as sqlalchemy
from sqlalchemy import create_engine
import pandas as pd

#API key connection
def Api_connect():
    Api_Id="AIzaSyCUDcS5HPA1D4QZOTvGSJd7aH8WeVogfOU"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_Id)
    return youtube
youtube=Api_connect()

#retrival of channel information 
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                 Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#retival of video ids from channel id
def get_video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    Playlist_Id=response['items'][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range (len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return video_ids
Video_Ids=get_video_ids("UCy1lBBbXhtfzugF_LK2b6Yw")

#get video information from the video ids

def get_video_info(video_ids):
    video_data=[]
    for video_id in Video_Ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                      Video_Id=item['id'],
                      Title=item['snippet']['title'],
                      Tags=item['snippet'].get('tags'),
                      Thumbnail=item['snippet']['thumbnails']['default']['url'],
                      Description=item['snippet'].get('description'),
                      Published_Date=item['snippet']['publishedAt'],
                      Duration=item['contentDetails']['duration'],
                      Views=item['statistics'].get('viewCount'),
                      likes=item['statistics'].get('likeCount'),
                      Comments=item['statistics'].get('commentCount'),
                      Favorite_Count=item['statistics']['favoriteCount'],
                      Definition=item['contentDetails']['definition'],
                      Caption_Status=item['contentDetails']['caption']
                      )
            video_data.append(data)
    return video_data

#get comment information for each video from video ids
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    
    return Comment_data

#upload to mongodb
client=pymongo.MongoClient ("mongodb+srv://ppamila3:jessjayl0609@cluster0.grirpvc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data02"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details02"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,
                      "comment_information":com_details})
    
    return "upload successful"

#table creaton,

def channel02():
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
    
    
    ch_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)
    
       
        
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
        
        
    connection=engine.connect()
    channel=pd.DataFrame(ch_list)
    channel.to_sql("channel02",engine,if_exists="replace",index=False)
    connection.close
    print("channel values inserted")
    
#video table

def video02():
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
    
    vi_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)
    
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
        
        
    connection=engine.connect()
    channel=pd.DataFrame(vi_list)
    channel.to_sql("video02",engine,if_exists="replace",index=False)
    connection.close
    print("video details inserted")
    
#comments table

def comment02():
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
    
    com_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)
    engine = create_engine('postgresql+psycopg2://postgres:12345@localhost/Youtube_data02')
        
        
    connection=engine.connect()
    channel=pd.DataFrame(com_list)
    channel.to_sql("comment02",engine,if_exists="replace",index=False)
    connection.close
    print("comment details inserted")
    
def tables():
    channel02()
    video02()
    comment02()
    return "Tables created successfully"

def show_channel02_table():
    ch_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    
    return df

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    
    return df3

#streamlit part
st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

options=st.selectbox("Skill Take Away",("Python Scripting","Data Collection","MongoDB","API Integration"))
   
    
    
channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data02"]
    coll1=db["channel_details02"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id in ch_ids:
        st.success("Channel details of the given channel ID already exist")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channel02_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#sql connection
mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="12345",
                            database="Youtube_data02",
                            port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. Channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each video",
                                              "5. videos with highest likes",
                                              "6. Likes of all videos",
                                              "7. views of each channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all videos in each channel",
                                              "10. Videos with highest number of comments"))


if question=="1. All the videos and the channel name":
    query1='''select Title as videos,Channel_Name as channelname from video02'''
    cursor.execute(query1)
    
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos","channelname"])
    st.write(df)

elif question=="2. Channels with most number of videos":
    query2='''select Channel_Name as channelname,Total_Videos as no_videos from channel02 
                order by Total_Videos desc'''
    cursor.execute(query2)
   
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channelname","no_videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select Views as views,Channel_Name as channelname,Title as videotitle from video02
                where Views is not null order by Views desc limit 10'''
    cursor.execute(query3)
    
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channelname","videotitle"])
    st.write(df3)

elif question=="4. comments in each video":
    query4='''select Comments as no_comments,Title as videotitle from video02 where Comments is not null'''
    cursor.execute(query4)
    
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no_comments","videotitle"])
    st.write(df4)

elif question=="5. videos with highest likes":
    query5='''select Title as videotitle,Channel_Name as channelname,likes as likecount
                from video02 where likes is not null order by likes desc'''
    cursor.execute(query5)
    
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. Likes of all videos":
    query6='''select likes as likecount,Title as videotitle from video02'''
    cursor.execute(query6)
    
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select Channel_Name as channelname,Views as totalviews from channel02'''
    cursor.execute(query7)
    
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)

elif question=="8. Videos published in the year of 2022":
    query8='''select Title as video_title,Published_Date as videorelease,Channel_Name as channelname from video02 
                where extract(year from Published_Date)=2022'''
    cursor.execute(query8)
    
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video_title","videorelease","channelname"])
    st.write(df8)

elif question=="9. Average duration of all videos in each channel":
    query9='''select Channel_Name as channelname,AVG(Duration) as averageduration from video02 group by Channel_Name'''
    cursor.execute(query9)
    
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    df9
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)

elif question==  "10. Videos with highest number of comments":
    query10='''select Title as videotitle,Channel_Name as channelname,Comments as comments from video02 where Comments is
                not null order by Comments desc'''
    cursor.execute(query10)
    
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)




        
        





    
