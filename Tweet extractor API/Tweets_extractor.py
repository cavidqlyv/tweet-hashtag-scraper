import tweepy as tw
from termcolor import colored
import csv
import os
import datetime
from datetime import timedelta  
from pprintpp import pprint as pp
from extraction_bib.api_utils import *
from extraction_bib.credentials import *
from extraction_bib.result_stream import *
import tweepy
import time

#####################################        DO NOT EDIT FOLLOWING STRINGS        #####################################
consumer_key = ''
consumer_secret = ''
bearer_token = ''
access_token = ''
access_token_secret = ''
#######################################################################################################################

#####################################                 LOGIN PART               ########################################
" Connection to twitter API using tweepy to use some methods of the library"
auth = tw.AppAuthHandler(consumer_key, consumer_secret)
Tweepy_Api = tw.API(auth, wait_on_rate_limit=True)
"Authentification to twitter API using extraction_bib"
search_args = load_credentials("twitter_keys.yaml", yaml_key="search_tweets_v2", env_overwrite=False)
#######################################################################################################################

#####################################                 PARAMETERS               ########################################
hashtags='#COVID19 OR #CORONAVIRUS OR #COVID OR #CORONA OR #CORONAVIRUSUPDATE OR #PANDEMIC'
twitter_query = f"({hashtags}) lang:en -is:retweet has:geo"
# see https://developer.twitter.com/en/docs/twitter-api/enterprise/search-api/guides/operators to build queries
Start_Year = 2020
Start_Month = 6
Start_Day = 1
End_Year = 2020
End_Month = 7
End_Day = 1
Start_Hour=0
chill_timer=1
Save_File_Name = "TweetsJun1toJul1"  # Name of the csv file where you want to save your tweets
enable_geo_tracking = True

#######################################################################################################################


#########################                      Start of the script               ######################################
Current_Year = Start_Year
Current_Month = Start_Month
Current_Day = Start_Day
Current_Hour = Start_Hour
header=True

Text = []
TwitID = []
Creations = []
Country = []
Country_code = []
Author_id = []
Like=[]
Reply=[]
Quote=[]
Retweet=[]
Geo=[]
Full_Location_name=[]
Number_of_Tweets = 0

def Build_Date(year, month, day, hour):
    if month > 9:
        string_month = str(month)
    else:
        string_month = '0' + str(month)
    if day > 9:
        string_day = str(day)
    else:
        string_day = '0' + str(day)
    if hour > 9:
        string_hour = str(hour)
    else:
        string_hour = '0' + str(hour)
    return str(year) + '-' + string_month + '-' + string_day + ' ' + string_hour + ':' + '00'

months_days = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def extract_hash_tags(s):
    return set(part[1:] for part in s.split() if part.startswith('#'))


while not ((Current_Day == End_Day) and (Current_Month == End_Month) and (Current_Year == End_Year)):
    #PRINT Current date
    t1 = datetime.datetime.now()
    print('---------------------- Request %s:%s -------------------------' % (t1.hour, t1.minute))
    
    #PRINT  current Date
    start = Build_Date(Current_Year, Current_Month, Current_Day, Current_Hour)
    print(colored(f'Start: {str(Current_Year)}-{str(Current_Month)}-{str(Current_Day)} {str(Current_Hour)}:00','green'))
    
    #Adding 1 hour to current hour
    #check the end of the day
    if Current_Hour + 1 == 24:
        Current_Hour = 0
        Current_Day = Current_Day + 1
        if Current_Day > months_days[Current_Month - 1]:
            Current_Day = 1
            if Current_Month + 1 > 12:
                Current_Month = 1
                Current_Year += 1
            else:
                Current_Month = Current_Month + 1
    else:
        Current_Hour = Current_Hour + 1

    #The end is satrt + 1 hour
    #PRINT current Date    
    end = Build_Date(Current_Year, Current_Month, Current_Day, Current_Hour)
    print(colored(f'End : {str(Current_Year)}-{str(Current_Month)}-{str(Current_Day)} {str(Current_Hour)}:00','red'))
    

    # #PRINT remaining Limit of tweets
    # rateStatus = Tweepy_Api.rate_limit_status()
    # valueoflimit=rateStatus['resources']['tweets']['/tweets/search/all']['remaining'],['values'][0]
    # print("Limit remaining to call ",valueoflimit[0])

    # time.sleep(1)

    #The Creation of query
    

    def call():
        try: 
            fields = 'geo,created_at,author_id,public_metrics'
            query = gen_request_parameters(twitter_query, start_time=start, end_time=end,
                                 tweet_fields=fields, results_per_call=500,
                                 expansions="geo.place_id", place_fields="country,country_code")
            tweets = collect_results(query, result_stream_args=search_args)
            return tweets
        except tweepy.TweepError:
            time.sleep(30)
            tweets = call()
            return tweets

    tweets=call()

    # collect places obj
    Places=[]
    nontweets=0
    for plcs in tweets:
        try:
            if plcs['places']!='':
                Places=Places+plcs['places']
                nontweets=nontweets+2
        except:
            continue

    try:
        # Geting Places from the responce
        
        print(len(Places))
        # Number of tweets
        print(colored(f"Number of raw tweets {len(tweets)-nontweets}",'yellow'))
    except:
        print(colored("No tweets",'red'))

        
    for tweet in tweets:
        try:
            if(tweet['geo']['place_id']==''):
                print(f' this tweet has no geo obj {tweets.index(tweet)}')
                raise Exception('No geo id')

            TempData={
                'geo':tweet['geo']['place_id'],
                'like_count':tweet['public_metrics']['like_count'],
                'reply_count':tweet['public_metrics']['reply_count'],
                'retweet_count':tweet['public_metrics']['retweet_count'],
                'quote_count':tweet['public_metrics']['quote_count'],
                'id':tweet['id'],
                'author_id':tweet['author_id'],
                'text':tweet['text'],
                'created_at':tweet['created_at']
            }

            try:
                past=False
                for place in Places:
                    # Checking the equality of the geolocations
                    if tweet['geo']['place_id']==place['id']:
                        past=True
                        #Country
                        ctr=place['country']
                        Country.append(ctr)
                        ctr=""
                        #Country_code
                        ccode=place['country_code']
                        Country_code.append(ccode)
                        ccode=""
                        #Country_location
                        cLocation=place['full_name']
                        Full_Location_name.append(cLocation)
                        cLocation=''

                        #extracted number of tweets
                        Number_of_Tweets += 1
                        break
                if past==False:
                    TempData={}
                    raise Exception()
            except:
                TempData={}
                raise Exception(f"not acceptable geo data{tweet['geo']['place_id']}")
            
            Geo.append(TempData['geo'])
            Like.append(TempData['like_count'])
            Reply.append(TempData['reply_count'])
            Retweet.append(TempData['retweet_count'])
            Quote.append(TempData['quote_count'])
            TwitID.append(TempData['id'])
            Author_id.append(TempData['author_id'])
            Text.append(TempData['text'])
            Creations.append(TempData['created_at'])
            

        except Exception as e:
            if tweets.index(tweet) <= len(tweets):
                print(colored(f'exception in tweet # {tweets.index(tweet)}  msg: {e} ','red'))
                TempData={}
    
    #PRINT We make exeption here because last element of tweets, is not a tweet
    #"~" is delimeter of this CSV file 
    print('---------------------- End of hour ---------------------------')
    with open(Save_File_Name + ".csv", "a", encoding='utf-8', newline='') as Document:
        if header:
            Document.write('Author_id~Creations~TwitID~Like~Reply~Quote~Retweet~Geo~Full_Location_name~Country~Country_code~Hashtags~Text\n')
            header=False

        writer = csv.writer(Document, delimiter="~")
        writer.writerows(
            [ Author_id[i], 
            Creations[i], TwitID[i], Like[i], Reply[i], Quote[i], Retweet[i], 
            Geo[i],
            Full_Location_name[i],
            Country[i],
            Country_code[i],
            extract_hash_tags(Text[i].replace('\n', ' ').replace('\r', ' ')),
            Text[i].replace('\n', ' ').replace('\r', ' ')
            ] for i in range(Number_of_Tweets)
            )
        Document.close()
    print('NUMBER OF TWEETS EXTRACTED : ' + str(Number_of_Tweets))
    
    Text = []
    TwitID = []
    Creations = []
    Country_code = []
    Author_id = []
    Like=[]
    Reply=[]
    Quote=[]
    Retweet=[]
    Geo=[]
    Full_Location_name=[]
    Number_of_Tweets = 0

    time.sleep(chill_timer)
    
