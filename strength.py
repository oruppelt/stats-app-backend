from fastapi import APIRouter
import pandas as pd
import numpy as np
from constants import SHEET_ID, DATA_TAB

router = APIRouter()

@router.get("/strength")
async def strength():
    try:
        URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={DATA_TAB}'
        df = pd.read_csv(URL)
        df.columns = ['Team1 ', 'Score1 ', 'Team2 ', 'Score2 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ', 'H1 ', 'H2 ', 'Color ']

        df.drop(['Color ', 'H1 ', 'H2 '], axis=1, inplace=True)
        # filter out match NaN
        df = df[df['Match '].notna()]
        df['Match '] = df['Match '].astype(int)

        df_swap = df.copy()
        df_swap.columns = ['Team2 ', 'Score2 ', 'Team1 ', 'Score1 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ']

        df_full = pd.concat([df, df_swap], axis=0)

        df_full['WIN'] = np.where(df_full['Score1 '] > df_full['Score2 '], 1, 0)
        df_full['WIN'] = np.where(df_full['Score1 '] == df_full['Score2 '], 0.5, df_full['WIN'])

        df_wins = pd.pivot_table(df_full, values='WIN', index=['Team1 '], columns=['Team2 '], aggfunc='sum')
        df_wins = df_wins.fillna(-1)

        df_alter = df_full.copy()

# select only the columns we need
        df_alter = df_alter[['Match ', 'Team1 ', 'Score1 ']]
        df_alter.columns = ['Match ', 'Alt_Team', 'Alt_Score']

        df_alt_merge = df_full.merge(df_alter, on='Match ', how='inner')
        df_alt_merge['Alt_WIN'] = np.where(df_alt_merge['Score1 '] > df_alt_merge['Alt_Score'], 1, 0)
        df_alt_merge['Alt_WIN'] = np.where(df_alt_merge['Score1 '] == df_alt_merge['Alt_Score'], 0.5, df_alt_merge['Alt_WIN'])

        # remove rows where Team1 = Alt_Team
        df_alt_merge = df_alt_merge[df_alt_merge['Team1 '] != df_alt_merge['Alt_Team']]

        # filter by team Memphis Grizzlies and match number = 1
        # df_alt_merge[(df_alt_merge['Team1 '] == 'Memphis Grizzlies') & (df_alt_merge['Match '] == 1)]

        df_alt_wins = pd.pivot_table(df_alt_merge, values='Alt_WIN', index=['Team1 '], columns=['Alt_Team'], aggfunc='sum')

        # divide by number of games played between each team
        df_alt_wins = df_alt_wins.div(pd.pivot_table(df_alt_merge, values='Alt_WIN', index=['Team1 '], columns=['Alt_Team'], aggfunc='count'), axis=1)

        # round up to 2 decimal places
        df_alt_wins = df_alt_wins.round(2)

        # add a column with row average
        df_alt_wins['Strength'] = df_alt_wins.mean(axis=1)

        # add rank column
        df_alt_wins['Rank'] = df_alt_wins['Strength'].rank(ascending=False).astype(int)

        # Fill NaN values with -1 to indicate "not applicable"
        df_alt_wins = df_alt_wins.fillna(-1)

        # Sort teams alphabetically
        teams = sorted([col for col in df_alt_wins.columns if col not in ['Strength', 'Rank']])

        # Reorder columns to match sorted teams (plus Strength and Rank at the end)
        df_alt_wins = df_alt_wins[teams + ['Strength', 'Rank']]

        # Format the data for frontend consumption
        matrix_data = df_alt_wins.reset_index().to_dict('records')

        matrix_data_wins = df_wins.reset_index().to_dict('records')

        return {
            "teams": teams,
            "matrix": matrix_data,
            "matrix_wins": matrix_data_wins,
            "metadata": {
                "maxWinRate": df_alt_wins[teams].max().max(),
                "minWinRate": df_alt_wins[teams].min().min(),
            }
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}
