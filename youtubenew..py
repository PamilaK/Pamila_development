import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build

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
db=client["Youtube_data01"]


def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    coll1=db["channel_details01"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,
                      "comment_information":com_details})
    
    return "upload successful"

#table creation for channel,video,comment
def channels01_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="12345",
                        database="youtube_data01",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels01'''
    cursor.execute(drop_query)
    mydb.commit()
    
    create_query='''create table   channels01(Channel_Name varchar(100),
                                                            Channel_Id varchar(80),
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
    cursor.execute(create_query)
    mydb.commit()
        
    
        
        
    ch_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)


    for index,row in df.iterrows():
        insert_query='''insert into channels01(Channel_Name ,
                                            Channel_Id,
                                            Subscribers,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        cursor.execute(insert_query,values)
        mydb.commit()
        
        



def videos_table():
    mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="12345",
                            database="youtube_data01",
                            port="5432")
    cursor=mydb.cursor()
    
    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query='''create table   videos( Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(50),
                                                        Title varchar(150),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Definition varchar(10),
                                                        Caption_Status varchar(50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
            insert_query='''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status
                                                )
                                                    
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                
            
                                                
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status']
                    )
            
            cursor.execute(insert_query,values)
            mydb.commit()


def comments_table():
    mydb=psycopg2.connect(host="localhost",
                                user="postgres",
                                password="12345",
                                database="youtube_data01",
                                port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()



    create_query='''create table   comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
            insert_query='''insert into comments(Comment_Id,
                                                    Video_Id ,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published
                                                )
                                                
                                                values(%s,%s,%s,%s,%s)'''
                                                
                    
            values=(row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )
                    

            cursor.execute(insert_query,values)
            mydb.commit()


#mongoDB to sql

def tables():
    channels01_table()
    videos_table()
    comments_table()
    
    
    return "Tables created successfully"


def show_channels01_table():
    ch_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    
    return df


def show_videos_table():
    vi_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)
    
    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)
    
    return df3


#streamlit part

st.set_page_config(page_title="My Youtube Project")
st.title(":red[MY FIRST PROJECT]")
st.header(":blue[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")
st.markdown("This project marks a pathway to set up a Streamlit web application in which we scrap the data from Youtube using API key, store the data in MongoDB and migrate all the data to the SQL. Finally we solve out some queries.")

channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data01"]
    coll1=db["channel_details01"]
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


    

#sql connection
mydb=psycopg2.connect(host="localhost",
                            user="postgres",
                            password="12345",
                            database="youtube_data01",
                            port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. List of channel names and all the corresponding videos",
                                              "2. Number of videos in each channel",
                                              "3. Top 10 most viewed videos in each channel",
                                              "4. Number of comments in each video and corresponding video names",
                                              "5. videos with highest likes in the corresponding channels",
                                              "6. Likes of all videos for each channel",
                                              "7. Channel name and the views of each channel",
                                              "8. Channel name with Videos published in the year of 2022",
                                              "9. The corresponding Average duration of all videos in each channel",
                                              "10. The appropriate Videos in the channel with highest number of comments"))


if question=="1. List of channel names and all the corresponding videos":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["video_title","channel name"])
    st.write(df)

elif question=="2. Number of videos in each channel":
    query2='''select channel_name as channelname,total_videos as no_videos from channels01 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel_name","no of videos"])
    st.write(df2)

elif question=="3. Top 10 most viewed videos in each channel":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
                where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. Number of comments in each video and corresponding video names":
    query4='''select comments as no_comments,title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. videos with highest likes in the corresponding channels":
    query5='''select title as videotitle,channel_name as channelname,Likes as likecount
                from videos where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. Likes of all videos for each channel":
    query6=('SELECT "title","likes" FROM videos')
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["videotitle","likecount"])
    st.write(df6)

elif question=="7. Channel name and the views of each channel":
    query7='''select channel_name as channelname,views as totalviews from channels01'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","views"])
    st.write(df7)

elif question=="8. Channel name with Videos published in the year of 2022":
    query8='''select title as video_title,published_date as videorelelease,channel_name as channelname from videos 
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. The corresponding Average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
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

elif question==  "10. The appropriate Videos in the channel with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from videos where comments is
                not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)




        
        




