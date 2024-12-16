import os
import pandas as pd
import psycopg2
import sqlite3
from datetime import datetime
import glob
import numpy as np
import logging
import argparse
from typing import Union, Any

# Set up logging
logging.basicConfig(level=logging.INFO)

def parse_args():
    parser = argparse.ArgumentParser(description='Load AFL data into a database')
    parser.add_argument('--db-type', choices=['postgres', 'sqlite'], default='sqlite',
                       help='Type of database to use (postgres or sqlite)')
    parser.add_argument('--db-path', default='test.sqlite3',
                       help='Path to SQLite database file (only used with sqlite)')
    parser.add_argument('--pg-host', default='localhost',
                       help='PostgreSQL host (only used with postgres)')
    parser.add_argument('--pg-dbname', default='postgres',
                       help='PostgreSQL database name (only used with postgres)')
    parser.add_argument('--pg-user', default='postgres',
                       help='PostgreSQL user (only used with postgres)')
    parser.add_argument('--pg-password', default='yourpassword',
                       help='PostgreSQL password (only used with postgres)')
    return parser.parse_args()

def get_db_connection(args) -> Union[psycopg2.extensions.connection, sqlite3.Connection]:
    """Create a database connection based on the specified type"""
    if args.db_type == 'postgres':
        return psycopg2.connect(
            dbname=args.pg_dbname,
            user=args.pg_user,
            password=args.pg_password,
            host=args.pg_host
        )
    else:  # sqlite
        return sqlite3.connect(args.db_path)

def get_schema_sql(db_type: str) -> dict[str, str]:
    """Return SQL statements for schema creation based on database type"""

    # Common column definitions
    players_columns = """
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        born_date DATE NOT NULL,
        debut_date DATE NOT NULL,
        height INTEGER,
        weight INTEGER,
        UNIQUE (first_name, last_name, born_date)
    """

    player_performances_columns = """
        player_id INTEGER REFERENCES players(id),
        team TEXT NOT NULL,
        year INTEGER NOT NULL,
        games_played INTEGER,
        opponent TEXT NOT NULL,
        round TEXT NOT NULL,
        result CHAR(1) NOT NULL,
        jersey_num INTEGER,
        kicks INTEGER,
        marks INTEGER,
        handballs INTEGER,
        disposals INTEGER,
        goals INTEGER,
        behinds INTEGER,
        hit_outs INTEGER,
        tackles INTEGER,
        rebound_50s INTEGER,
        inside_50s INTEGER,
        clearances INTEGER,
        clangers INTEGER,
        free_kicks_for INTEGER,
        free_kicks_against INTEGER,
        brownlow_votes INTEGER,
        contested_possessions INTEGER,
        uncontested_possessions INTEGER,
        contested_marks INTEGER,
        marks_inside_50 INTEGER,
        one_percenters INTEGER,
        bounces INTEGER,
        goal_assist INTEGER,
        percentage_of_game_played INTEGER
    """

    team_lineups_columns = """
        year INTEGER NOT NULL,
        date TIMESTAMP NOT NULL,
        round_num TEXT NOT NULL,
        team_name TEXT NOT NULL,
        player_name TEXT NOT NULL,
        UNIQUE (date, team_name, player_name)
    """

    matches_columns = """
        year INTEGER NOT NULL,
        round_num TEXT NOT NULL,
        date TIMESTAMP NOT NULL,
        venue TEXT NOT NULL,
        team_1_name TEXT NOT NULL,
        team_2_name TEXT NOT NULL,
        team_1_q1_goals INTEGER NOT NULL,
        team_1_q1_behinds INTEGER NOT NULL,
        team_1_q2_goals INTEGER NOT NULL,
        team_1_q2_behinds INTEGER NOT NULL,
        team_1_q3_goals INTEGER NOT NULL,
        team_1_q3_behinds INTEGER NOT NULL,
        team_1_final_goals INTEGER NOT NULL,
        team_1_final_behinds INTEGER NOT NULL,
        team_2_q1_goals INTEGER NOT NULL,
        team_2_q1_behinds INTEGER NOT NULL,
        team_2_q2_goals INTEGER NOT NULL,
        team_2_q2_behinds INTEGER NOT NULL,
        team_2_q3_goals INTEGER NOT NULL,
        team_2_q3_behinds INTEGER NOT NULL,
        team_2_final_goals INTEGER NOT NULL,
        team_2_final_behinds INTEGER NOT NULL,
        UNIQUE (date, team_1_name, team_2_name)
    """

    team_stats_columns = """
        year INTEGER NOT NULL,
        team TEXT NOT NULL,
        kicks INTEGER,
        marks INTEGER,
        handballs INTEGER,
        disposals INTEGER,
        goals INTEGER,
        behinds INTEGER,
        hit_outs INTEGER,
        tackles INTEGER,
        rebound_50s INTEGER,
        inside_50s INTEGER,
        clearances INTEGER,
        clangers INTEGER,
        frees_for INTEGER,
        brownlow_votes INTEGER,
        contested_possessions INTEGER,
        uncontested_possessions INTEGER,
        contested_marks INTEGER,
        marks_inside_50 INTEGER,
        one_percenters INTEGER,
        bounces INTEGER,
        goal_assists INTEGER,
        UNIQUE (year, team)
    """

    # Database-specific ID fields
    id_field = "id INTEGER PRIMARY KEY AUTOINCREMENT" if db_type == 'sqlite' else "id SERIAL PRIMARY KEY"
    cascade = " CASCADE" if db_type == 'postgres' else ""

    return {
        'players': f"""
            CREATE TABLE IF NOT EXISTS players (
                {id_field},
                {players_columns}
            )
        """,
        'player_performances': f"""
            CREATE TABLE IF NOT EXISTS player_performances (
                {id_field},
                {player_performances_columns}
            )
        """,
        'team_lineups': f"""
            CREATE TABLE IF NOT EXISTS team_lineups (
                {id_field},
                {team_lineups_columns}
            )
        """,
        'matches': f"""
            CREATE TABLE IF NOT EXISTS matches (
                {id_field},
                {matches_columns}
            )
        """,
        'team_stats': f"""
            CREATE TABLE IF NOT EXISTS team_stats (
                {id_field},
                {team_stats_columns}
            )
        """,
        'drop_players': f"DROP TABLE IF EXISTS players{cascade}",
        'drop_player_performances': f"DROP TABLE IF EXISTS player_performances{cascade}",
        'drop_team_lineups': f"DROP TABLE IF EXISTS team_lineups{cascade}",
        'drop_matches': f"DROP TABLE IF EXISTS matches{cascade}",
        'drop_team_stats': f"DROP TABLE IF EXISTS team_stats{cascade}"
    }

def create_schema(conn):
    """Create the database schema if it doesn't exist"""
    db_type = 'sqlite' if isinstance(conn, sqlite3.Connection) else 'postgres'
    sql = get_schema_sql(db_type)

    # Create cursor differently based on database type
    if db_type == 'sqlite':
        cur = conn.cursor()
    else:
        cur = conn.cursor().__enter__()  # For PostgreSQL's context manager

    try:
        # Drop existing tables
        for table in ['players', 'player_performances', 'team_lineups', 'matches', 'team_stats']:
            cur.execute(sql[f'drop_{table}'])

        # Create tables
        for table in ['players', 'player_performances', 'team_lineups', 'matches', 'team_stats']:
            cur.execute(sql[table])

        # Create indexes (same for both databases)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_lineups_team_date
            ON team_lineups (team_name, date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_lineups_player
            ON team_lineups (player_name)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_matches_teams
            ON matches (team_1_name, team_2_name)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_matches_date
            ON matches (date)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_matches_year_round
            ON matches (year, round_num)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_stats_team
            ON team_stats (team)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_team_stats_year
            ON team_stats (year)
        """)

        conn.commit()
    finally:
        if db_type == 'sqlite':
            cur.close()
        else:
            cur.__exit__(None, None, None)  # For PostgreSQL's context manager

def parse_date(date_str):
    """Convert date from DD-MM-YYYY format to YYYY-MM-DD"""
    if pd.isna(date_str):
        return None
    return datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')

def convert_to_int(value):
    """Convert a value to integer, handling floats and NaN/None values"""
    if pd.isna(value):
        return None
    # Handle float strings like "3.0" and float values
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def load_player_data(conn, directory_path):
    """Load all player data from CSV files in the specified directory"""
    # Get all personal details files
    personal_files = glob.glob(os.path.join(directory_path, '*_personal_details.csv'))
    db_type = 'sqlite' if isinstance(conn, sqlite3.Connection) else 'postgres'

    for personal_file in personal_files:
        logging.info(f"Processing {personal_file}")

        # Load personal details
        personal_df = pd.read_csv(personal_file)

        # Get corresponding performance file
        performance_file = personal_file.replace('_personal_details.csv', '_performance_details.csv')
        if not os.path.exists(performance_file):
            logging.info(f"Warning: No performance data found for {personal_file}")
            continue

        performance_df = pd.read_csv(performance_file)

        cur = conn.cursor()
        try:
            # Insert player
            cur.execute("""
                INSERT INTO players (first_name, last_name, born_date, debut_date, height, weight)
                VALUES (?, ?, ?, ?, ?, ?)
            """ if db_type == 'sqlite' else """
                INSERT INTO players (first_name, last_name, born_date, debut_date, height, weight)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                str(personal_df.iloc[0]['first_name']),
                str(personal_df.iloc[0]['last_name']),
                parse_date(personal_df.iloc[0]['born_date']),
                parse_date(personal_df.iloc[0]['debut_date']),
                convert_to_int(personal_df.iloc[0]['height']),
                convert_to_int(personal_df.iloc[0]['weight'])
            ))

            if db_type == 'sqlite':
                player_id = cur.lastrowid
            else:
                player_id = cur.fetchone()[0]

            # Insert performances
            for _, row in performance_df.iterrows():
                values = [player_id]

                # String columns
                values.extend([
                    str(row['team']),
                    convert_to_int(row['year']),
                    convert_to_int(row['games_played']),
                    str(row['opponent']),
                    str(row['round']),
                    str(row['result']),
                ])

                # Numeric columns
                numeric_columns = [
                    'jersey_num', 'kicks', 'marks', 'handballs', 'disposals',
                    'goals', 'behinds', 'hit_outs', 'tackles', 'rebound_50s',
                    'inside_50s', 'clearances', 'clangers', 'free_kicks_for',
                    'free_kicks_against', 'brownlow_votes', 'contested_possessions',
                    'uncontested_possessions', 'contested_marks', 'marks_inside_50',
                    'one_percenters', 'bounces', 'goal_assist', 'percentage_of_game_played'
                ]

                for col in numeric_columns:
                    values.append(convert_to_int(row.get(col)))

                placeholders = '?' if db_type == 'sqlite' else '%s'
                cur.execute(f"""
                    INSERT INTO player_performances (
                        player_id, team, year, games_played, opponent, round, result,
                        jersey_num, kicks, marks, handballs, disposals, goals, behinds,
                        hit_outs, tackles, rebound_50s, inside_50s, clearances, clangers,
                        free_kicks_for, free_kicks_against, brownlow_votes,
                        contested_possessions, uncontested_possessions, contested_marks,
                        marks_inside_50, one_percenters, bounces, goal_assist,
                        percentage_of_game_played
                    )
                    VALUES ({','.join([placeholders] * len(values))})
                """, tuple(values))

            conn.commit()
        finally:
            cur.close()

def load_team_lineups(conn, directory_path):
    """Load all team lineup data from CSV files in the specified directory"""
    lineup_files = glob.glob(os.path.join(directory_path, 'team_lineups_*.csv'))
    db_type = 'sqlite' if isinstance(conn, sqlite3.Connection) else 'postgres'
    placeholder = '?' if db_type == 'sqlite' else '%s'

    for lineup_file in lineup_files:
        logging.info(f"Processing {lineup_file}")
        df = pd.read_csv(lineup_file)

        cur = conn.cursor()
        try:
            for _, row in df.iterrows():
                try:
                    # Split the players string into individual names
                    player_names = str(row['players']).split(';')

                    # Handle round_num as string instead of trying to convert to int
                    round_num = str(row['round_num']).strip()

                    # Insert a record for each player in the lineup
                    for player_name in player_names:
                        cur.execute(f"""
                            INSERT INTO team_lineups (year, date, round_num, team_name, player_name)
                            VALUES ({placeholder}, {placeholder}, {placeholder}, {placeholder}, {placeholder})
                            ON CONFLICT (date, team_name, player_name) DO NOTHING
                        """, (
                            convert_to_int(row['year']),
                            row['date'],  # Already in YYYY-MM-DD HH:MM format
                            round_num,
                            str(row['team_name']),
                            player_name.strip()
                        ))
                except Exception as e:
                    logging.error(f"Error processing row in {lineup_file}:")
                    logging.error(f"Row data: {row}")
                    logging.error(f"Error: {str(e)}")
                    continue

            conn.commit()
        finally:
            cur.close()

def load_matches(conn, directory_path):
    """Load all match data from CSV files in the specified directory"""
    match_files = glob.glob(os.path.join(directory_path, 'matches_*.csv'))
    db_type = 'sqlite' if isinstance(conn, sqlite3.Connection) else 'postgres'
    placeholder = '?' if db_type == 'sqlite' else '%s'

    for match_file in match_files:
        logging.info(f"Processing {match_file}")
        df = pd.read_csv(match_file)

        cur = conn.cursor()
        try:
            for _, row in df.iterrows():
                values = (
                    int(row['year']),
                    str(row['round_num']).strip(),
                    row['date'],
                    str(row['venue']),
                    str(row['team_1_team_name']),
                    str(row['team_2_team_name']),
                    int(row['team_1_q1_goals']),
                    int(row['team_1_q1_behinds']),
                    int(row['team_1_q2_goals']),
                    int(row['team_1_q2_behinds']),
                    int(row['team_1_q3_goals']),
                    int(row['team_1_q3_behinds']),
                    int(row['team_1_final_goals']),
                    int(row['team_1_final_behinds']),
                    int(row['team_2_q1_goals']),
                    int(row['team_2_q1_behinds']),
                    int(row['team_2_q2_goals']),
                    int(row['team_2_q2_behinds']),
                    int(row['team_2_q3_goals']),
                    int(row['team_2_q3_behinds']),
                    int(row['team_2_final_goals']),
                    int(row['team_2_final_behinds'])
                )

                cur.execute(f"""
                    INSERT INTO matches (
                        year, round_num, date, venue,
                        team_1_name, team_2_name,
                        team_1_q1_goals, team_1_q1_behinds,
                        team_1_q2_goals, team_1_q2_behinds,
                        team_1_q3_goals, team_1_q3_behinds,
                        team_1_final_goals, team_1_final_behinds,
                        team_2_q1_goals, team_2_q1_behinds,
                        team_2_q2_goals, team_2_q2_behinds,
                        team_2_q3_goals, team_2_q3_behinds,
                        team_2_final_goals, team_2_final_behinds
                    )
                    VALUES ({','.join([placeholder] * len(values))})
                """, values)

            conn.commit()
        finally:
            cur.close()

def load_team_stats(conn, directory_path):
    """Load all team statistics from CSV files in the specified directory"""
    stats_files = glob.glob(os.path.join(directory_path, 'team_stats_*.csv'))
    db_type = 'sqlite' if isinstance(conn, sqlite3.Connection) else 'postgres'
    placeholder = '?' if db_type == 'sqlite' else '%s'

    for stats_file in stats_files:
        logging.info(f"Processing {stats_file}")
        df = pd.read_csv(stats_file)

        cur = conn.cursor()
        try:
            for _, row in df.iterrows():
                values = []

                # Add year and team first
                values.extend([
                    convert_to_int(row['year']),
                    str(row['team'])
                ])

                # Add all the numeric columns, handling potential NaN values
                numeric_columns = [
                    'kicks', 'marks', 'handballs', 'disposals', 'goals', 'behinds',
                    'hit_outs', 'tackles', 'rebound_50s', 'inside_50s', 'clearances',
                    'clangers', 'frees_for', 'brownlow_votes', 'contested_possessions',
                    'uncontested_possessions', 'contested_marks', 'marks_inside_50',
                    'one_percenters', 'bounces', 'goal_assists'
                ]

                for col in numeric_columns:
                    values.append(convert_to_int(row.get(col)))

                cur.execute(f"""
                    INSERT INTO team_stats (
                        year, team, kicks, marks, handballs, disposals, goals, behinds,
                        hit_outs, tackles, rebound_50s, inside_50s, clearances, clangers,
                        frees_for, brownlow_votes, contested_possessions,
                        uncontested_possessions, contested_marks, marks_inside_50,
                        one_percenters, bounces, goal_assists
                    )
                    VALUES ({','.join([placeholder] * len(values))})
                """, tuple(values))

            conn.commit()
        finally:
            cur.close()

def main():
    args = parse_args()
    conn = get_db_connection(args)

    try:
        # Create schema
        create_schema(conn)

        # Load all different types of data
        load_player_data(conn, "data/players")
        load_team_lineups(conn, "data/lineups")
        load_matches(conn, "data/matches")
        load_team_stats(conn, "data/teams")

    finally:
        conn.close()

if __name__ == "__main__":
    main()