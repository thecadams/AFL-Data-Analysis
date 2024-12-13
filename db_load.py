import os
import pandas as pd
import psycopg2
from datetime import datetime
import glob
import numpy as np
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_schema(conn):
    """Create the database schema if it doesn't exist"""
    with conn.cursor() as cur:
        # Drop the old players table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS players CASCADE;
        """)

        # Create players table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                born_date DATE NOT NULL,
                debut_date DATE NOT NULL,
                height INTEGER,  -- in cm
                weight INTEGER,  -- in kg
                UNIQUE (first_name, last_name, born_date)
            );
        """)

        # Drop the old player_performances table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS player_performances CASCADE;
        """)

        # Create player_performances table with nullable games_played
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_performances (
                id SERIAL PRIMARY KEY,
                player_id INTEGER REFERENCES players(id),
                team VARCHAR(100) NOT NULL,
                year INTEGER NOT NULL,
                games_played INTEGER,  -- Now nullable
                opponent VARCHAR(100) NOT NULL,
                round VARCHAR(20) NOT NULL,
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
            );
        """)

        # Drop the old team_lineups table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS team_lineups CASCADE;
        """)

        # Create team_lineups table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_lineups (
                id SERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                date TIMESTAMP NOT NULL,
                round_num VARCHAR(20) NOT NULL,  -- Changed from INTEGER to VARCHAR
                team_name VARCHAR(100) NOT NULL,
                player_name VARCHAR(100) NOT NULL,
                UNIQUE (date, team_name, player_name)
            );

            CREATE INDEX IF NOT EXISTS idx_team_lineups_team_date
            ON team_lineups (team_name, date);

            CREATE INDEX IF NOT EXISTS idx_team_lineups_player
            ON team_lineups (player_name);
        """)

        # Drop the old matches table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS matches CASCADE;
        """)

        # Create matches table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches (
                id SERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                round_num VARCHAR(20) NOT NULL,  -- Changed from INTEGER to VARCHAR
                date TIMESTAMP NOT NULL,
                venue VARCHAR(100) NOT NULL,
                team_1_name VARCHAR(100) NOT NULL,
                team_2_name VARCHAR(100) NOT NULL,
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
            );

            CREATE INDEX IF NOT EXISTS idx_matches_teams
            ON matches (team_1_name, team_2_name);

            CREATE INDEX IF NOT EXISTS idx_matches_date
            ON matches (date);

            CREATE INDEX IF NOT EXISTS idx_matches_year_round
            ON matches (year, round_num);
        """)

        # Drop the old team_stats table if it exists
        cur.execute("""
            DROP TABLE IF EXISTS team_stats CASCADE;
        """)

        # Create team_stats table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS team_stats (
                id SERIAL PRIMARY KEY,
                year INTEGER NOT NULL,
                team VARCHAR(100) NOT NULL,
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
            );

            CREATE INDEX IF NOT EXISTS idx_team_stats_team
            ON team_stats (team);

            CREATE INDEX IF NOT EXISTS idx_team_stats_year
            ON team_stats (year);
        """)

        conn.commit()

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

        with conn.cursor() as cur:
            # Insert player
            cur.execute("""
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

            player_id = cur.fetchone()[0]

            # Insert performances
            for _, row in performance_df.iterrows():
                # Convert numeric values, handling floats appropriately
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

                cur.execute("""
                    INSERT INTO player_performances (
                        player_id, team, year, games_played, opponent, round, result,
                        jersey_num, kicks, marks, handballs, disposals, goals, behinds,
                        hit_outs, tackles, rebound_50s, inside_50s, clearances, clangers,
                        free_kicks_for, free_kicks_against, brownlow_votes,
                        contested_possessions, uncontested_possessions, contested_marks,
                        marks_inside_50, one_percenters, bounces, goal_assist,
                        percentage_of_game_played
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(values))

            conn.commit()

def load_team_lineups(conn, directory_path):
    """Load all team lineup data from CSV files in the specified directory"""
    # Get all team lineup files
    lineup_files = glob.glob(os.path.join(directory_path, 'team_lineups_*.csv'))

    for lineup_file in lineup_files:
        logging.info(f"Processing {lineup_file}")
        df = pd.read_csv(lineup_file)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                try:
                    # Split the players string into individual names
                    player_names = str(row['players']).split(';')

                    # Handle round_num as string instead of trying to convert to int
                    round_num = str(row['round_num']).strip()

                    # Insert a record for each player in the lineup
                    for player_name in player_names:
                        cur.execute("""
                            INSERT INTO team_lineups (year, date, round_num, team_name, player_name)
                            VALUES (%s, %s, %s, %s, %s)
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

def load_matches(conn, directory_path):
    """Load all match data from CSV files in the specified directory"""
    # Get all match files
    match_files = glob.glob(os.path.join(directory_path, 'matches_*.csv'))

    for match_file in match_files:
        logging.info(f"Processing {match_file}")
        df = pd.read_csv(match_file)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # Convert numpy types to Python native types
                values = (
                    int(row['year']),
                    # Handle round_num as string instead of trying to convert to int
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

                cur.execute("""
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
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, values)

            conn.commit()

def load_team_stats(conn, directory_path):
    """Load all team statistics from CSV files in the specified directory"""
    # Get all team stats files
    stats_files = glob.glob(os.path.join(directory_path, 'team_stats_*.csv'))

    for stats_file in stats_files:
        logging.info(f"Processing {stats_file}")
        df = pd.read_csv(stats_file)

        with conn.cursor() as cur:
            for _, row in df.iterrows():
                # Convert numpy values to Python native types and handle NaN
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

                cur.execute("""
                    INSERT INTO team_stats (
                        year, team, kicks, marks, handballs, disposals, goals, behinds,
                        hit_outs, tackles, rebound_50s, inside_50s, clearances, clangers,
                        frees_for, brownlow_votes, contested_possessions,
                        uncontested_possessions, contested_marks, marks_inside_50,
                        one_percenters, bounces, goal_assists
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(values))

            conn.commit()

def main():
    # Update these connection parameters as needed
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="yourpassword",
        host="localhost"
    )

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