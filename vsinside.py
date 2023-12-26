from googleapiclient.discovery import build
import pandas as pd
import psycopg2
import mysql.connector
import streamlit as st
import plotly.express as px
import pymongo

#API Key connection

def Api_connect():
    Api_id= "AIzaSyCzmXX3ROMywfhpjCg4lg3pvwB5eINybkA"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_id)

    return youtube

youtube = Api_connect()
    
    
#get channel information

def get_channel_info(channel_id):

    request = youtube.channels().list(
                                part ="snippet,ContentDetails,statistics",
                                id = channel_id
            
        )
    response = request.execute()
    for i in response["items"]:
        data = dict(Channel_Name = i["snippet"]["title"],
                    Channel_Id = i["id"],
                    Subscriber = i["statistics"]["subscriberCount"],
                    Views = i["statistics"]["viewCount"],
                    Total_Videos = i["statistics"]["videoCount"],
                    Channel_Description = i["snippet"]["description"],
                    Playlist_Id =i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data


#get video ids

def get_videos_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(
                                    id = channel_id,
                                    part = "contentDetails"
    ).execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1= youtube.playlistItems().list(
                                            part ='snippet',
                                            playlistId = Playlist_Id,
                                            maxResults = 50,
                                            pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")
        
        if next_page_token is None:
            break

    return video_ids

#get Video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
                                    part = "snippet, ContentDetails, statistics",
                                    id = video_id)
        response = request.execute()

        for item in response['items']:
            data = dict(Channel_Name = item["snippet"]["channelTitle"],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Thumbnails = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']['description'],
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics']['commentCount'],
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_status = item['contentDetails']['caption'])
                        
            video_data.append(data)

    return video_data     


#get comment information
def get_comment_info(video_ids):  

    Comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                                                    part = "snippet",
                                                    videoId = video_id,
                                                    maxResults = 50
                )
            response = request.execute()

            for item in response["items"]:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']["videoId"],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published= item['snippet']['topLevelComment']['snippet']['publishedAt'])
                            

                Comment_data.append(data)
    except:
        pass

    return Comment_data

#get playlist details

def get_playlist_details(channel_id):
    next_page_token = None
    All_data=[]

    while True:
        request = youtube.playlists().list(
                                        part ="snippet,contentDetails",
                                        channelId = channel_id,
                                        maxResults = 50,
                                        pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id=item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name= item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount'])
            
            All_data.append(data)

        next_page_token= response.get('nextPageToken')
        if next_page_token is None:
            break
    return All_data

#upload to Mongodb

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client['youtube_data']


def channel_details(channel_id):
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details = get_video_info(vi_ids)
    com_details = get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information" :ch_details,
                      "playlist_information": pl_details,
                      "video_information":vi_details,
                      "comment_information" :com_details})
    
    return "upload completed successfully"


import mysql.connector 

def channels_table():
    mydb = mysql.connector.connect(
                        host = "127.0.0.1",
                        user = "root",
                        password = "Sujithra@20",
                        database = "youtube_data")
    cursor=mydb.cursor()

    drop_query =  '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    
    create_query = '''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscriber bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
                                                            
    cursor.execute(create_query)
    mydb.commit()
        
    


    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscriber,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
                                            
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscriber'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        
        cursor.execute(insert_query,values)
        mydb.commit()
            
        

def playlist_table():
    mydb = mysql.connector.connect(
                            host = "127.0.0.1",
                            user = "root",
                            password = "Sujithra@20",
                            database = "youtube_data")
    cursor=mydb.cursor()

    drop_query =  '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query = '''create table if not exists playlists(Playlist_Id varchar(100)primary key,
                                                                Title varchar(100),
                                                                Channel_Id varchar(80),
                                                                Channel_Name varchar(100),
                                                                PublishedAt varchar(80),
                                                                Video_Count int)'''
                                                                
                                                    
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
            
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
                                                
        values = (row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count'])
        
        cursor.execute(insert_query,values)
        mydb.commit()
        
def video_table():
        mydb = mysql.connector.connect(
                                host = "127.0.0.1",
                                user = "root",
                                password = "Sujithra@20",
                                database = "youtube_data")
        cursor=mydb.cursor()

        drop_query =  '''drop table if exists videos'''
        cursor.execute(drop_query)
        mydb.commit()


        create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                                Channel_Id varchar(100),
                                                                Video_Id varchar(100) primary key,
                                                                Title varchar(150),
                                                                Thumbnails varchar(200),
                                                                Description text,
                                                                Published_Date varchar(100),
                                                                Duration varchar(50),
                                                                Views int,
                                                                Likes bigint,
                                                                Comments int,
                                                                Favorite_Count int,
                                                                Definition varchar(100),
                                                                Caption_status varchar(50))'''
                                                                
                                                                        
                                                        
        cursor.execute(create_query)
        mydb.commit()

        vi_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
                
        df2=pd.DataFrame(vi_list)

        for index,row in df2.iterrows():
                insert_query = '''insert into videos(Channel_Name,
                                                        Channel_Id,
                                                        Video_Id,
                                                        Title,
                                                        Thumbnails,
                                                        Description,
                                                        Published_Date,
                                                        Duration,
                                                        Views,
                                                        Likes,
                                                        Comments,
                                                        Favorite_Count,
                                                        Definition,
                                                        Caption_status)
                                                        
                                                        
                                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                        
                values = (row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Title'],
                        row['Thumbnails'],
                        row['Description'],
                        row['Published_Date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite_Count'],
                        row['Definition'],
                        row['Caption_status'])
                        
                
                cursor.execute(insert_query,values)
                mydb.commit()


def comment_table():
        mydb = mysql.connector.connect(
                                host = "127.0.0.1",
                                user = "root",
                                password = "Sujithra@20",
                                database = "youtube_data")
        cursor=mydb.cursor()

        drop_query =  '''drop table if exists comments'''
        cursor.execute(drop_query)
        mydb.commit()


        create_query = '''create table if not exists comments(Comment_Id varchar(150) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published varchar(50))'''
                                                                
                                                        
        cursor.execute(create_query)
        mydb.commit()

        com_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])
                
        df3=pd.DataFrame(com_list)

        for index,row in df3.iterrows():
                insert_query = '''insert into comments(Comment_Id,
                                                        Video_Id,
                                                        Comment_Text,
                                                        Comment_Author,
                                                        Comment_Published)
                                                        
                                                        
                                                        values(%s,%s,%s,%s,%s)'''
                                                        
                values=(row['Comment_Id'],
                        row['Video_Id'],
                        row['Comment_Text'],
                        row['Comment_Author'],
                        row['Comment_Published'])
                        
                cursor.execute(insert_query,values)
                mydb.commit()



def tables():
    channels_table()
    playlist_table()
    video_table()
    comment_table()
    
    return "Tables Created Successfully"

def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df

def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
            
    df1=st.dataframe(pl_list)
    return df1


def show_videos_table():     
        vi_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data["video_information"][i])
                
        df2=st.dataframe(vi_list)
        
        return df2
    
def show_comments_table():
        com_list=[]
        db=client["youtube_data"]
        coll1=db["channel_details"]
        for com_data in coll1.find({},{"_id":0,"comment_information":1}):
                for i in range(len(com_data["comment_information"])):
                        com_list.append(com_data["comment_information"][i])
                
        df3=st.dataframe(com_list)
        return df3
    

#streamlit part

with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel Id")

if st.button("Collect and store Data"):
    ch_ids = []    
    db=client["youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id in ch_ids:
        st.success("Channel details of given channel ID already exists")
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    Table = tables()
    st.success(Table)
    
show_table=st.radio("Select the table for view",("Channels","Playlists","Videos","Comments"))

if show_table=="Channels":
    show_channels_table()
    
elif show_table=="Playlists":
    show_playlists_table()
    
elif show_table=="Videos":
    show_videos_table()
    
elif show_table=="Comments":
    show_comments_table()

    
#SQL Connections
mydb = mysql.connector.connect(
                                host = "127.0.0.1",
                                user = "root",
                                password = "Sujithra@20",
                                database = "youtube_data")
cursor=mydb.cursor()

question = st.selectbox("Select Your Question",("1. All the Videos and Channel Name",
                                                "2. Channels with most number of videos",
                                                "3. 10 most viewed videos",
                                                "4. Comments in each videos",
                                                "5. Videos with highest likes",
                                                "6. Likes of all videos",
                                                "7. Views of each channel",
                                                "8. videos published in the year of 2022",
                                                "9. Average duration of all videos in each channel",
                                                "10.Videos with Highest number of comments"))

if question =="1. All the Videos and Channel Name":
    cursor.execute("""SELECT title AS Video_Title, channel_name AS Channel_Name
                                FROM videos
                                ORDER BY channel_name""")
    df = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df)
    
elif question ==  "2. Channels with most number of videos":
    cursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
    df1 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df1)
    
    st.write("### :green[Number of videos in each channel :]")
    #st.bar_chart(df1,x= cursor.column_names[0],y= cursor.column_names[1])
    fig = px.bar(df1,
                x=cursor.column_names[0],
                y=cursor.column_names[1],
                orientation='v',
                color=cursor.column_names[0]
                    )
    st.plotly_chart(fig,use_container_width=True) 


elif question ==  "3. 10 most viewed videos":
    cursor.execute("""SELECT channel_name AS Channel_Name, title AS Video_Title, views AS Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
    df2 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df2)
    
elif question ==  "4. Comments in each videos":
    cursor.execute("""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
    df3 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df3)
    
elif question == "5. Videos with highest likes":
    cursor.execute("""SELECT channel_name AS Channel_Name,title AS Title,likes AS Likes_Count 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
    df4 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df4)

elif question == "6. Likes of all videos":
    cursor.execute("""SELECT title AS Title, likes AS Likes_Count
                            FROM videos
                            ORDER BY likes DESC""")
    df5 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df5)

elif question == "7. Views of each channel":
    cursor.execute("""SELECT channel_name AS Channel_Name, views AS Views
                            FROM channels
                            ORDER BY views DESC""")
    df6 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df6)
    
    st.write("### :green[Channels vs Views :]")
    fig = px.bar(df6,
                     x=cursor.column_names[0],
                     y=cursor.column_names[1],
                     orientation='v',
                     color=cursor.column_names[0]
                    )
    st.plotly_chart(fig,use_container_width=True)
    
elif question == "8. videos published in the year of 2022":
    cursor.execute("""SELECT channel_name AS Channel_Name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
    df7 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df7)
    
elif question == "9. Average duration of all videos in each channel":
    cursor.execute("""SELECT channel_name AS Channel_Name,
                            AVG(duration)/60 AS "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
    df8 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df8)
    
elif question == "10.Videos with Highest number of comments":
    cursor.execute("""SELECT channel_name AS Channel_Name,video_id AS Video_ID,comments AS Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
    df9 = pd.DataFrame(cursor.fetchall(),columns=cursor.column_names)
    st.write(df9)

    
    
    



