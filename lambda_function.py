from datetime import datetime, timedelta

from contextlib import contextmanager
import os
import sys

import boto3
import psycopg2
from psycopg2 import sql
import psycopg2.extras

from liquiaoe.loaders import HttpsLoader as Loader
from liquiaoe.managers import TournamentManager, Tournament

# will timeout if check too many tournaments
MAX_LOAD = 400

TIERS = ('A-Tier', 'B-Tier', 'C-Tier', 'S-Tier', 'Qualifier',)


class ReglistTournament:
    """ Holder class for generating proper tournament insert values."""
    insert_template_sql = """INSERT INTO tournaments
(name, liquipedia_url, rules_url, info_url, format, game, tier, prize_pool, organizers, start_date, end_date, created_at, updated_at)
VALUES %s
ON CONFLICT DO NOTHING
"""
    now = datetime.now()
    def __init__(self, tournament):
        self.tournament = tournament
        self.mismatches = list()
        self.flags = ''

    def insert_attributes(self):
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

    def changes(self, db_attributes):
        start_date, end_date, prize_pool, status, flags = db_attributes
        if status == 1:
            return
        cancelled = False
        if start_date != self.tournament.start and 'check dates' not in (flags or ''):
            if flags:
                self.flags = f'{flags}:check dates'
            else:
                self.flags = 'check dates'
        if end_date != self.tournament.end:
            self.mismatches.append(('end_date', end_date, self.tournament.end,))
        if prize_pool != self.tournament.prize and prize_pool.startswith('$') and self.tournament.prize:
            self.mismatches.append(('prize_pool', prize_pool, self.tournament.prize,))
        return self.mismatches or self.tournament.cancelled or self.flags

    def update_and_flag(self):
        if self.tournament.cancelled:
            sql = 'UPDATE tournaments SET status = 1 WHERE liquipedia_url = %s'
            values = [self.tournament.url]
            with execute_sql(sql, values, True):
                return
        changes = []
        for column, old, new in self.mismatches:
            if not new:
                continue
            changes.append((column, new,))
        if changes:
            update_statement = 'UPDATE tournaments SET {columns} WHERE liquipedia_url = %s'.format(
                columns=', '.join(['{}=%s'.format(change[0]) for change in changes]))
            values = [change[1] for change in changes]
            values.append(self.tournament.url)
            with execute_sql(update_statement, values, True):
                pass
        if self.flags:
            with execute_sql("UPDATE tournaments SET flags=%s WHERE liquipedia_url = %s",
                             [self.flags, self.tournament.url], True):
                pass

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

@contextmanager
def execute_sql(sql, values, commit=False):
    conn = db_connection()
    cursor = conn.cursor()
    if commit:
        yield
        cursor.execute("BEGIN")
        cursor.execute(sql, values)
        cursor.execute("COMMIT")
    else:
        cursor.execute(sql, values)
        yield cursor
    conn.close()

def upcoming_saved_tournaments(timebox):
    """ Set of liquipedia urls of tournaments already in db to avoid reduplication"""
    done = {}
    sql = f"SELECT liquipedia_url, start_date, end_date, prize_pool, status, flags from tournaments WHERE liquipedia_url IS NOT NULL"
    with execute_sql(sql, []) as cursor:
        for row in cursor.fetchall():
            done[row[0]] = row[1:]
    return done


def execute_bulk_insert(sql, values):
    """ Takes insert sql with an array of values to be inserted"""
    conn = db_connection()
    cur = conn.cursor()
    cur.execute("BEGIN")
    psycopg2.extras.execute_values(cur, sql, values)
    cur.execute("COMMIT")
    conn.close()

def save_upcoming_tournaments(timebox):
    """ Check liquipedia for new upcoming tournaments and save them."""
    upcoming = upcoming_saved_tournaments(timebox)
    tm = TournamentManager(Loader())
    new_tournaments = list()
    changed_tournaments = list()
    for game, tournaments in tm.starting(timebox).items():
        for tournament in tournaments:
            reglist_tournament = ReglistTournament(tournament)
            if tournament.tier not in TIERS:
                continue
            if tournament.url in upcoming:
                if reglist_tournament.changes(upcoming[tournament.url]):
                    changed_tournaments.append(reglist_tournament)
            elif len(new_tournaments) < MAX_LOAD:
                tournament.load_advanced(tm.loader)
                new_tournaments.append(reglist_tournament.insert_attributes())

    for tournament in changed_tournaments:
        tournament.update_and_flag()

    execute_bulk_insert(ReglistTournament.insert_template_sql, new_tournaments)
    return [t[0] for t in new_tournaments], [t.tournament.name for t in changed_tournaments]

def email_results(results):
    message = f"Liquipedia check resulted in {results['new']} new tournaments and {results['updated']} updated tournaments"
    email_message(message)

def email_message(message):
    print(message)
    return
    client = boto3.client('ses', region_name='us-east-2')
    destination = os.environ.get('RECIPIENT')
    print(message)
    client.send_email(
        Destination={
            'ToAddresses': [destination]
        },
        Message={
            'Body': {
                'Text': {
                    'Charset': 'UTF-8',
                    'Data': message,
                }
            },
            'Subject': {
                'Charset': 'UTF-8',
                'Data': 'Liquipedia Check Results',
            },
        },
        Source='support@tourneyopportunities.net'
    )

def handler(event, context):
    """ Function to be called by AWS Lambda."""
    print('Beginning handler function')
    try:
        now = datetime.now().date()
        timebox = [now, now + timedelta(days=600),]
        new, updated = save_upcoming_tournaments(timebox)
        to_return = { 'new': new, 'updated': updated }
        email_results(to_return)
        print(to_return)
    except Exception as e:
        to_return = f"ERROR: {e}"
        email_message(to_return)
    return to_return

if __name__ == '__main__':
    now = datetime.now().date()
    print(handler(None, None))
