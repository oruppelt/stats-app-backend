from fastapi import APIRouter
import pandas as pd
import numpy as np
from constants import SHEET_ID, DATA_TAB
from logger_config import setup_logging, log_data_shape, log_error_with_context
from datetime import datetime

router = APIRouter()
logger = setup_logging("INFO")

@router.get("/schedule_strength")
async def strength():
    start_time = datetime.now()
    logger.info("Starting schedule_strength calculation endpoint")
    
    try:
        # Log Google Sheets URL construction
        URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={DATA_TAB}'
        logger.info(f"Fetching data from Google Sheets URL: {URL}")
        
        # Fetch data with error handling
        try:
            df = pd.read_csv(URL)
            logger.info("Successfully fetched CSV data from Google Sheets")
            log_data_shape(logger, df, "initial_fetch", "raw CSV data")
        except Exception as fetch_error:
            log_error_with_context(logger, fetch_error, {
                "url": URL,
                "sheet_id": SHEET_ID,
                "data_tab": DATA_TAB
            })
            raise
        
        # Validate and set column names
        original_columns = list(df.columns)
        logger.info(f"Original CSV columns: {original_columns}")
        
        expected_columns = ['Team1 ', 'Score1 ', 'Team2 ', 'Score2 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ', 'H1 ', 'H2 ', 'Color ']
        if len(df.columns) != len(expected_columns):
            logger.warning(f"Column count mismatch! Expected {len(expected_columns)} columns, got {len(df.columns)}")
        
        df.columns = expected_columns
        log_data_shape(logger, df, "column_assignment", "after setting column names")

        # Drop unnecessary columns
        columns_to_drop = ['Color ', 'H1 ', 'H2 ']
        df.drop(columns_to_drop, axis=1, inplace=True)
        logger.info(f"Dropped columns: {columns_to_drop}")
        log_data_shape(logger, df, "column_drop", "after dropping unnecessary columns")
        
        # Filter out Match NaN values
        initial_rows = len(df)
        df = df[df['Match '].notna()]
        filtered_rows = len(df)
        logger.info(f"Filtered out {initial_rows - filtered_rows} rows with NaN Match values")
        log_data_shape(logger, df, "match_filter", "after filtering NaN Match values")
        
        # Convert Match column to int
        try:
            df['Match '] = df['Match '].astype(int)
            logger.info("Successfully converted Match column to integer")
        except Exception as convert_error:
            log_error_with_context(logger, convert_error, {
                "match_column_type": str(df['Match '].dtype),
                "sample_match_values": list(df['Match '].unique()[:10])
            })
            raise

        # Create swapped dataframe for bidirectional analysis
        df_swap = df.copy()
        df_swap.columns = ['Team2 ', 'Score2 ', 'Team1 ', 'Score1 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ']
        logger.info("Created swapped dataframe for schedule strength analysis")
        log_data_shape(logger, df_swap, "df_swap", "swapped columns dataframe")

        # Concatenate dataframes
        df_full = pd.concat([df, df_swap], axis=0)
        logger.info("Concatenated original and swapped dataframes")
        log_data_shape(logger, df_full, "concatenation", "full bidirectional dataset")

        # Calculate WIN column
        df_full['WIN'] = np.where(df_full['Score1 '] > df_full['Score2 '], 1, 0)
        df_full['WIN'] = np.where(df_full['Score1 '] == df_full['Score2 '], 0.5, df_full['WIN'])
        win_stats = df_full['WIN'].value_counts().to_dict()
        logger.info(f"WIN calculation statistics: {win_stats}")

        # Create wins pivot table
        try:
            df_wins = pd.pivot_table(df_full, values='WIN', index=['Team1 '], columns=['Team2 '], aggfunc='sum')
            logger.info("Successfully created wins pivot table")
            logger.info(f"Wins pivot table shape: {df_wins.shape}")
        except Exception as pivot_error:
            log_error_with_context(logger, pivot_error, {
                "pivot_operation": "wins_table",
                "unique_team1_count": len(df_full['Team1 '].unique()),
                "unique_team2_count": len(df_full['Team2 '].unique()),
            })
            raise

        # Create alternative team data for schedule strength calculation
        df_alter = df_full.copy()
        logger.info("Created copy for alternative team analysis")

        # Select only the columns we need for alternative comparison
        df_alter = df_alter[['Match ', 'Team1 ', 'Score1 ']]
        df_alter.columns = ['Match ', 'Alt_Team', 'Alt_Score']
        logger.info("Prepared alternative team data with renamed columns")
        log_data_shape(logger, df_alter, "alt_team_prep", "alternative team dataset")

        # Merge with original data for cross-team comparison
        try:
            df_alt_merge = df_full.merge(df_alter, on='Match ', how='inner')
            logger.info("Successfully merged original data with alternative team data")
            log_data_shape(logger, df_alt_merge, "alt_merge", "merged alternative analysis dataset")
        except Exception as merge_error:
            log_error_with_context(logger, merge_error, {
                "merge_operation": "alternative_teams",
                "df_full_matches": len(df_full['Match '].unique()),
                "df_alter_matches": len(df_alter['Match '].unique()),
            })
            raise

        # Calculate alternative WIN rates
        df_alt_merge['Alt_WIN'] = np.where(df_alt_merge['Score1 '] > df_alt_merge['Alt_Score'], 1, 0)
        df_alt_merge['Alt_WIN'] = np.where(df_alt_merge['Score1 '] == df_alt_merge['Alt_Score'], 0.5, df_alt_merge['Alt_WIN'])
        logger.info("Calculated Alt_WIN column for schedule strength comparison")

        # Remove rows where Team1 = Alt_Team (self-comparison)
        initial_alt_rows = len(df_alt_merge)
        df_alt_merge = df_alt_merge[df_alt_merge['Team1 '] != df_alt_merge['Alt_Team']]
        filtered_alt_rows = len(df_alt_merge)
        logger.info(f"Removed {initial_alt_rows - filtered_alt_rows} self-comparison rows")
        log_data_shape(logger, df_alt_merge, "self_filter", "after removing self-comparisons")

        # Create alternative wins pivot table
        try:
            df_alt_wins = pd.pivot_table(df_alt_merge, values='Alt_WIN', index=['Team1 '], columns=['Alt_Team'], aggfunc='sum')
            logger.info("Successfully created alternative wins pivot table")
            logger.info(f"Alternative wins pivot table shape: {df_alt_wins.shape}")
        except Exception as alt_pivot_error:
            log_error_with_context(logger, alt_pivot_error, {
                "pivot_operation": "alternative_wins",
                "unique_teams": len(df_alt_merge['Team1 '].unique()),
                "unique_alt_teams": len(df_alt_merge['Alt_Team'].unique()),
            })
            raise

        # Normalize by number of games played between each team
        try:
            game_counts = pd.pivot_table(df_alt_merge, values='Alt_WIN', index=['Team1 '], columns=['Alt_Team'], aggfunc='count')
            df_alt_wins = df_alt_wins.div(game_counts, axis=1)
            logger.info("Successfully normalized alternative wins by game counts")
        except Exception as norm_error:
            log_error_with_context(logger, norm_error, {"operation": "normalization_by_game_counts"})
            raise

        # Round to 2 decimal places
        df_alt_wins = df_alt_wins.round(2)
        logger.info("Rounded alternative win rates to 2 decimal places")

        # Fill NaN values with -1
        # df_alt_wins = df_alt_wins.fillna(-1)
        # logger.info("Filled NaN values in alternative wins with -1")

        # Calculate schedule strength difference
        try:
            df_diff = df_wins - df_alt_wins
            logger.info("Successfully calculated schedule strength difference matrix")
            log_data_shape(logger, df_diff, "schedule_diff", "schedule strength difference matrix")
        except Exception as diff_error:
            log_error_with_context(logger, diff_error, {
                "operation": "schedule_difference",
                "wins_shape": df_wins.shape,
                "alt_wins_shape": df_alt_wins.shape
            })
            raise

        # Calculate overall strength score
        df_diff['Strength'] = df_diff.sum(axis=1)
        logger.info("Calculated overall schedule strength scores")

        # Add rank column
        df_diff['Rank'] = df_diff['Strength'].rank(ascending=False).astype(int)
        logger.info("Added ranking based on schedule strength")

        # Log strength statistics
        strength_stats = df_diff['Strength'].describe()
        logger.info(f"Schedule strength statistics: min={strength_stats['min']:.3f}, max={strength_stats['max']:.3f}, mean={strength_stats['mean']:.3f}")

        # Fill NaN values with -1
        df_diff = df_diff.fillna(-1)
        logger.info("Filled remaining NaN values with -1")

        # Sort teams alphabetically
        teams = sorted([col for col in df_diff.columns if col not in ['Strength', 'Rank']])
        logger.info(f"Found {len(teams)} teams for final output")

        # Reorder columns to match sorted teams
        df_diff = df_diff[teams + ['Strength', 'Rank']]

        # Format the data for frontend consumption
        try:
            matrix_data = df_diff.reset_index().to_dict('records')
            df_wins = df_wins.fillna(-1)
            matrix_data_wins = df_wins.reset_index().to_dict('records')
            
            logger.info(f"Successfully formatted response data: {len(matrix_data)} team records")
            
            # Log total processing time
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Schedule strength calculation completed successfully in {duration:.3f}s")
            
            return {
                "teams": teams,
                "matrix": matrix_data,
                "matrix_wins": matrix_data_wins
            }
        except Exception as format_error:
            log_error_with_context(logger, format_error, {
                "operation": "response_formatting",
                "diff_matrix_shape": df_diff.shape,
                "wins_matrix_shape": df_wins.shape
            })
            raise
            
    except Exception as e:
        # Log the error with full context
        duration = (datetime.now() - start_time).total_seconds()
        log_error_with_context(logger, e, {
            "endpoint": "schedule_strength",
            "processing_time": f"{duration:.3f}s",
            "error_type": type(e).__name__
        })
        logger.error(f"Schedule strength calculation failed after {duration:.3f}s")
        return {"error": str(e)}
