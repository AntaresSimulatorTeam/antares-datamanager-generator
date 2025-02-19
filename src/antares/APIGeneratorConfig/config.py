import os

ANTARES_API_HOST= "https://antares-web-recette.rte-france.com"
ANTARES_API_TOKEN="eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ7XCJpZFwiOiAxMTIsIFwidHlwZVwiOiBcImJvdHNcIiwgXCJpbXBlcnNvbmF0b3JcIjogMiwgXCJncm91cHNcIjogW3tcImlkXCI6IFwiNDE5OTgzMWYtMGU5Ny00MjkwLTkxYjItZjlhY2Y5ZWY3MzM0XCIsIFwibmFtZVwiOiBcIkZvcm1hdGlvblwiLCBcInJvbGVcIjogMzB9LCB7XCJpZFwiOiBcInRlc3RcIiwgXCJuYW1lXCI6IFwidGVzdFwiLCBcInJvbGVcIjogNDB9XX0iLCJpYXQiOjE3MTIzMjYwODUsIm5iZiI6MTcxMjMyNjA4NSwianRpIjoiMWZkZmM5ODktMGIwMy00Yjk3LWFlZDEtODgwMjkyMzU4NDliIiwiZXhwIjo4MDcxMzY2MDg1LCJ0eXBlIjoiYWNjZXNzIiwiZnJlc2giOmZhbHNlfQ.S9Snc1QRfWqQ0kxqcHk_vys75T_pkQYpdgsfDmYIwQU"

#TODO to be managed as ENV variables !!
class APIGeneratorConfig:
    def __init__(self):
        self.host = ANTARES_API_HOST
        self.token = ANTARES_API_TOKEN
        self.verify_ssl = False

api_config = APIGeneratorConfig()
