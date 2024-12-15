from fastapi import APIRouter
import pandas as pd
import numpy as np
from constants import SHEET_ID, DATA_TAB

router = APIRouter()

@router.get("/for_against")
async def for_against():
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

        df_scores = df_full.groupby(['Team1 '])[['Score1 ', 'Score2 ']].sum().reset_index()
        df_scores.columns = ['Team', 'Scored For', 'Scored Against']

        # Convert DataFrame to list of dictionaries for JSON response
        scores_list = df_scores.to_dict('records')
        median_scored_for = np.median(df_scores['Scored For'])
        median_scored_against = np.median(df_scores['Scored Against'])

        return {"df_scores": scores_list, "median_scored_for": median_scored_for, "median_scored_against": median_scored_against}
    except Exception as e:
        print(f"Error: {str(e)}")
        return {"error": str(e)}