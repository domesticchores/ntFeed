import psycopg
from pathlib import Path
import os
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
import pytz
import schedule
import time

LocalDir = Path(__file__).parent
os.chdir(LocalDir)

DBuser = os.environ['dbuser']
DBpass = os.environ['dbpass']
REDDIT_RSS_URL = os.environ['redditurl']
NTFY_URL = os.environ['ntfyurl']
MINUTES_BEFORE_LOOP = os.environ['minutesbeforeloop']

print(DBuser,DBpass,REDDIT_RSS_URL,NTFY_URL,MINUTES_BEFORE_LOOP)

#define headers for requests to use; predefined for ease of use
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
}

localTZ = datetime.now(timezone.utc).astimezone().tzinfo

class rssFeed:
    def __init__(self, rss_url, headers):
        print("Initiating RSS Reader...")
        self.url = rss_url
        self.headers = headers

        try:
            self.connection = psycopg.connect(dbname="rssFeed",user=DBuser,password=DBpass)
            with self.connection.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS posts (
                        id text PRIMARY KEY,
                        title text,
                        price text,
                        type text,
                        url text,
                        pubdate timestamp)
                    """)
        except Exception as e:
            print("An error occured whilst initializing database:")
            print(e)
            quit()
        # use requests module to download the rss feed data
        try:
            self.r = requests.get(rss_url, headers=self.headers)
            self.status_code = self.r.status_code
        except Exception as e:
            print("Error fetching the URL: ", rss_url)
            print(e)
            quit()
        
        try:
            self.soup = BeautifulSoup(self.r.text, features="xml")
        except Exception as e:
            print("Could not parse the xml: ", self.url)
            print(e)
            quit()
        
        # once rss file is downloaded, use bs4 to parse posts into easily usable forms
        self.posts = self.soup.findAll("entry")
        self.postDict = [
            {
                "id": a.find("id").text,
                "title": a.find("title").text[
                    a.find("title").text.find("]")
                    + 2 : a.find("title").text.find("$")
                    - 1
                ]
                + " ",
                "price": (a.find("title").text + " ")[
                    a.find("title")
                    .text.find("$") : a.find("title")
                    .text.find(" ", a.find("title").text.find("$"))
                ]
                or "$???",
                "type": a.find("title").text[
                    a.find("title").text.find("[")
                    + 1 : a.find("title").text.find("]", a.find("title").text.find("["))
                ]
                or "UNKNOWN",
                "url": a.find("content").text[
                    a.find("content").text.find("<span><a href=")
                    + 15 : a.find("content").text.find(
                        '"', a.find("content").text.find("<span><a href=") + 15
                    )
                ],
                "redditlink": a.find("link").get("href"),
                "pubdate": datetime.strptime(
                    a.find("published").text[:19], "%Y-%m-%dT%H:%M:%S"
                ),
            }
        for a in self.posts
        ]
        toBeUploaded = []
        for post in self.postDict:
            if rssFeed.findPost(post.get("id"), self.connection.cursor()) == None:
                rssFeed.addPost(post, self.connection)
                toBeUploaded.append(post)
            else:
                print(
                    "encountered post already stored, hit all new posts since last call! "
                    + post.get("id")
                )
                break
        print(len(toBeUploaded))
        if len(toBeUploaded) < 15:
            i = len(toBeUploaded)-1
        else:
            print("too many posts, only pushing 5 latest")
            i = 4
        while i >= 0:
            rssFeed.sendNotif(toBeUploaded[i])
            i-=1
        # commit data for persistent database
        self.connection.commit()
        self.connection.close()
        print(
            "cycle completed! ("
            + datetime.now().strftime("%m/%d/%y @ %I:%M %p")
            + ")\n"
        )
        return

    def intOrFloat(string):
        try:
            return float(string)
        except Exception as e:
            return int(string)
    
    def findPost(ID, cursor):
        cursor.execute("SELECT * FROM posts where id=%(id)s", {'id': ID})
        return cursor.fetchone()

    def addPost(Post, connection):
        data = {
            'id': Post.get("id"),
            'title': Post.get("title"),
            'price': Post.get("price"),
            'type': Post.get("type"),
            'url': Post.get("url"),
            'pubdate': Post.get("pubdate"),
        }
        connection.cursor().execute(
                """INSERT INTO posts (id, title, price, type, url, pubdate) VALUES (%(id)s,
                %(title)s,
                %(price)s,
                %(type)s,
                %(url)s,
                %(pubdate)s)""", data)

    def sendNotif(Post):
        print("New post found! ID: " + Post.get("id"))
        return
        linkTo = Post.get("url")
        redTo = Post.get("redditlink")
        try:
            requests.post(
                NTFY_URL,
                data=Post.get("title")
                + "\n"
                + Post.get("url")
                + "\n("
                + pytz.timezone("UTC")
                .localize(Post.get("pubdate"))
                .astimezone(tz=localTZ)
                .strftime("%m/%d/%y @ %I:%M %p")
                + ")",
                headers={
                    "Title": "New Sale: ["
                    + Post.get("type")
                    + ", "
                    + Post.get("price")
                    + "]",
                    "Priority": "default",
                    "Tags": "computer",
                    "Actions": f"view, Product Link, {linkTo}; view, Post Link, {redTo}",
                },
            )
        except Exception as e:
            print("Error sending out notification to ntfy: ")
            print(e)

if __name__ == "__main__":
    rssFeed(REDDIT_RSS_URL, headers)
    schedule.every(int(MINUTES_BEFORE_LOOP)).minutes.do(rssFeed, REDDIT_RSS_URL, headers)
    while True:
        schedule.run_pending()
        time.sleep(1)
