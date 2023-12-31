from datetime import datetime, timedelta

import os
import sys

import psycopg2
import psycopg2.extras

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import TournamentManager, Tournament

# will timeout if check too many tournaments
MAX_LOAD = 4

TIERS = ('A-Tier', 'B-Tier', 'C-Tier', 'S-Tier', 'Qualifier',)


class ReglistTournament:
    """ Holder class for generating proper tournament insert values."""
    template_sql = """INSERT INTO tournaments
(name, liquipedia_url, rules_url, info_url, format, game, tier, prize_pool, organizers, start_date, end_date, created_at, updated_at)
VALUES %s
"""
    now = datetime.now()
    def __init__(self, tournament):
        self.tournament = tournament

    def info(self):
        """ Array of values for batch insert"""
        rules_url = info_url = None
        for link in self.tournament.links:
            if link['type'] == 'rules':
                rules_url = link['href']
            elif link['type'] == 'aoezone':
                info_url = link['href']
            elif not info_url and link['type'] == 'home':
                info_url = link['href']
        if not info_url:
            info_url = f"https://liquipedia.net{self.tournament.url}"
        return [
            self.tournament.name,
            self.tournament.url,
            rules_url,
            info_url,
            self.tournament.format_style,
            self.tournament.game,
            self.tournament.tier,
            self.tournament.prize,
            ', '.join(self.tournament.organizers),
            self.tournament.start,
            self.tournament.end,
            ReglistTournament.now,
            ReglistTournament.now,
            ]

def db_host():
    """ Utility function """
    return os.environ.get('DB_HOST')
    
def db_name():
    """ Utility function """
    return os.environ.get('DB_NAME')

def db_password():
    """ Utility function """
    return os.environ.get('DB_PASSWORD')

def db_connection():
    return psycopg2.connect(database=db_name(), user="postgres", password=db_password(), host=db_host())

def upcoming_saved_tournaments(timebox):
    """ Set of liquipedia urls of tournaments already in db to avoid reduplication"""
    cutoff = timebox[0] - timedelta(days=10)
    done = set()
    sql = f"SELECT liquipedia_url from tournaments WHERE start_date > '{cutoff}' AND liquipedia_url IS NOT NULL"
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    for row in cursor.fetchall():
        done.add(row[0])
    return done
        
def execute_bulk_insert(sql, values):
    """ Takes insert sql with an array of values to be inserted"""
    conn = db_connection()
    cur = conn.cursor()
    cur.execute("BEGIN")
    psycopg2.extras.execute_values(cur, sql, values)
    cur.execute("COMMIT")

def save_upcoming_tournaments(timebox):
    """ Check liquipedia for new upcoming tournaments and save them."""
    skip = upcoming_saved_tournaments(timebox)
    tm = TournamentManager(Loader())
    new_tournaments = list()
    try:
        for game, tournaments in tm.starting(timebox).items():
            for tournament in tournaments:
                if tournament.url in skip or tournament.tier not in TIERS:
                    continue
                tournament.load_advanced(tm.loader)
                new_tournaments.append(ReglistTournament(tournament).info())
                if len(new_tournaments) >= MAX_LOAD:
                    raise StopIteration
    except StopIteration:
        pass
    execute_bulk_insert(ReglistTournament.template_sql, new_tournaments)
    return len(new_tournaments)
    
def handler(event, context):
    """ Function to be called by AWS Lambda."""
    now = datetime.now().date()
    timebox = [now, now + timedelta(days=600),]
    return save_upcoming_tournaments(timebox)

if __name__ == '__main__':
    now = datetime.now().date()
    timebox = [now, now + timedelta(days=600),]
    print(handler(None, None))
