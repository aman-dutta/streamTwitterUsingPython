from __future__ import absolute_import, print_function

# Import modules
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import dataset
from sqlalchemy.exc import ProgrammingError

consumer_key = "kavGSyen6dwUgbIvaZQDSnZEp"
consumer_secret = "yPLlso1J5ZvzF8oNlq3sHs9TrFrmqqBdtyf8qZE8pDTo9PnxNa"
access_token = "3283386410-NKfmoNF3pLDt0jcdPHlT5SbJ5L0H003vJFqbnd8"
access_token_secret = "KW0a8zpXOrH0zWybYdt9elWj5MCTlabGprKkNKwFdMJaV"

class StdOutListener(StreamListener):
    def on_status(self, status):
        print(status.text)
        if status.retweeted:
            return
        
        id_str = status.id_str
        created = status.created_at
        text = status.text
        fav = status.favorite_count
        name = status.user.screen_name
        description = status.user.description
        loc = status.user.location
        user_created = status.user.created_at
        followers = status.user.followers_count

        table = db['myTable']

        try:
            table.insert(dict(
                id_str=id_str,
                created=created,
                text=text,
                fav_count=fav,
                user_name=name,
                user_description=description,
                user_location=loc,
                user_created=user_created,
                user_followers=followers,
            ))
        except ProgrammingError as err:
            print(err)
    
    def on_error(self, status_code):
        if status_code == 420:      #When rate limit is reached
            return False


if __name__ == '__main__':
    db = dataset.connect("sqlite:///tweets.db")
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['Reliance', 'Jio'])