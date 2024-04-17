import requests
from bs4 import BeautifulSoup

results_ua_url = 'https://www.flashscore.com/football/ukraine/premier-league/results/'

get = requests.get(results_ua_url)
results_html = get.text

sp = BeautifulSoup(results_html, 'html.parser')


h = open('UkrainianPremierLeagueResultsFootballUkraine.html', 'r')
sp = BeautifulSoup(h, 'html.parser')
# t = sp.find('div', class_='container__livetable')
# container__fsbody = t.find('div', class_='container__fsbody')
# tb = container__fsbody.find('div', id='live-table')
# print(tb.find_all('div', class_='wclLeagueHeader wclLeagueHeader--collapsed wclLeagueHeader--noCheckBox wclLeagueHeader--indent'))
table = sp.find("div", ["sportName", "soccer"])

def with_id(tag):
    return tag.has_attr('id')

rounds = table.find_all(with_id)

def get_info(round):
    try:
        date = round.find('div', class_='event__time').text
        home_name = round.find('div', class_='event__participant event__participant--home').text
        home_score = round.find('div', class_='event__score event__score--home').text
        away_name = round.find('div', class_='event__participant event__participant--away').text
        away_score = round.find('div', class_='event__score event__score--away').text
    except Exception as e:
        return {'e': e}
    return {
        "date": date,
        "home_name": home_name,
        "home_score": home_score,
        "away_name": away_name,
        "away_score": away_score
    }

result = []

for round in rounds:
    result.append(get_info(round=round))

print(result)