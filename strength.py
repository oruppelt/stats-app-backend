from fastapi import APIRouter, Response
import pandas as pd
import numpy as np
from constants import SHEET_ID, DATA_TAB
from logger_config import setup_logging, log_data_shape, log_error_with_context
from datetime import datetime
from cache import get_cache

router = APIRouter()
logger = setup_logging("INFO")
cache = get_cache()

async def _compute_strength():
    """Internal function to compute strength data (for cache.get_or_compute)"""
    try:
        # Log Google Sheets URL construction
        URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={DATA_TAB}'
        logger.info(f"Fetching data from Google Sheets URL: {URL}")
        
        # Fetch data with error handling
        try:
            df = pd.read_csv(URL)
            logger.info(f"Successfully fetched CSV data from Google Sheets")
            log_data_shape(logger, df, "initial_fetch", "raw CSV data")
        except Exception as fetch_error:
            log_error_with_context(logger, fetch_error, {
                "url": URL,
                "sheet_id": SHEET_ID,
                "data_tab": DATA_TAB
            })
            raise
        
        # Log original columns for debugging sheet structure changes
        original_columns = list(df.columns)
        logger.info(f"Original CSV columns: {original_columns}")
        
        # Set expected column names and validate
        expected_columns = ['Team1 ', 'Score1 ', 'Team2 ', 'Score2 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ', 'H1 ', 'H2 ', 'Color ']
        if len(df.columns) != len(expected_columns):
            logger.warning(f"Column count mismatch! Expected {len(expected_columns)} columns, got {len(df.columns)}")
            logger.warning(f"Expected: {expected_columns}")
            logger.warning(f"Actual: {original_columns}")
        
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
        
        # Convert Match column to int with error handling
        try:
            df['Match '] = df['Match '].astype(int)
            logger.info("Successfully converted Match column to integer")
        except Exception as convert_error:
            unique_match_values = df['Match '].unique()[:10]  # Log first 10 unique values
            log_error_with_context(logger, convert_error, {
                "match_column_type": str(df['Match '].dtype),
                "sample_match_values": list(unique_match_values),
                "total_unique_matches": len(df['Match '].unique())
            })
            raise

        # Create swapped dataframe for bidirectional analysis
        df_swap = df.copy()
        df_swap.columns = ['Team2 ', 'Score2 ', 'Team1 ', 'Score1 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ']
        logger.info("Created swapped dataframe for bidirectional matchup analysis")
        log_data_shape(logger, df_swap, "df_swap", "swapped columns dataframe")

        # Concatenate original and swapped dataframes
        df_full = pd.concat([df, df_swap], axis=0)
        logger.info("Concatenated original and swapped dataframes")
        log_data_shape(logger, df_full, "concatenation", "full bidirectional dataset")

        # Calculate WIN column (1 for win, 0 for loss, 0.5 for tie)
        df_full['WIN'] = np.where(df_full['Score1 '] > df_full['Score2 '], 1, 0)
        df_full['WIN'] = np.where(df_full['Score1 '] == df_full['Score2 '], 0.5, df_full['WIN'])
        
        # Log WIN calculation statistics
        win_stats = df_full['WIN'].value_counts().to_dict()
        logger.info(f"WIN calculation statistics: {win_stats}")
        log_data_shape(logger, df_full, "win_calculation", "after adding WIN column")

        # Create wins pivot table
        try:
            df_wins = pd.pivot_table(df_full, values='WIN', index=['Team1 '], columns=['Team2 '], aggfunc='sum')
            logger.info("Successfully created wins pivot table")
            logger.info(f"Wins pivot table shape: {df_wins.shape}")
            df_wins = df_wins.fillna(-1)
            logger.info("Filled NaN values in wins pivot table with -1")
        except Exception as pivot_error:
            log_error_with_context(logger, pivot_error, {
                "pivot_operation": "wins_table",
                "unique_team1_count": len(df_full['Team1 '].unique()),
                "unique_team2_count": len(df_full['Team2 '].unique()),
            })
            raise

        # Create alternative team data for strength calculation
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
        logger.info("Calculated Alt_WIN column for strength comparison")

        # Remove rows where Team1 = Alt_Team (self-comparison)
        initial_alt_rows = len(df_alt_merge)
        df_alt_merge = df_alt_merge[df_alt_merge['Team1 '] != df_alt_merge['Alt_Team']]
        filtered_alt_rows = len(df_alt_merge)
        logger.info(f"Removed {initial_alt_rows - filtered_alt_rows} self-comparison rows")
        log_data_shape(logger, df_alt_merge, "self_filter", "after removing self-comparisons")

        # Create alternative wins pivot table for strength calculation
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

        # Calculate team strength as row average
        df_alt_wins['Strength'] = df_alt_wins.mean(axis=1)
        logger.info("Calculated team strength as row average")
        
        # Log strength statistics
        strength_stats = df_alt_wins['Strength'].describe()
        logger.info(f"Strength statistics: min={strength_stats['min']:.3f}, max={strength_stats['max']:.3f}, mean={strength_stats['mean']:.3f}")

        # Add rank column
        df_alt_wins['Rank'] = df_alt_wins['Strength'].rank(ascending=False).astype(int)
        logger.info("Added ranking based on strength")

        # Fill NaN values with -1 to indicate "not applicable"
        df_alt_wins = df_alt_wins.fillna(-1)
        logger.info("Filled NaN values with -1")

        # Sort teams alphabetically
        teams = sorted([col for col in df_alt_wins.columns if col not in ['Strength', 'Rank']])
        logger.info(f"Found {len(teams)} teams for final output")

        # Reorder columns to match sorted teams (plus Strength and Rank at the end)
        df_alt_wins = df_alt_wins[teams + ['Strength', 'Rank']]

        # Format the data for frontend consumption
        try:
            matrix_data = df_alt_wins.reset_index().to_dict('records')
            matrix_data_wins = df_wins.reset_index().to_dict('records')
            
            # Calculate metadata
            team_columns = [col for col in df_alt_wins.columns if col not in ['Strength', 'Rank']]
            max_win_rate = df_alt_wins[team_columns].max().max()
            min_win_rate = df_alt_wins[team_columns].min().min()
            
            logger.info(f"Successfully formatted response data: {len(matrix_data)} team records")
            logger.info(f"Win rate range: {min_win_rate:.3f} to {max_win_rate:.3f}")
            
            result = {
                "teams": teams,
                "matrix": matrix_data,
                "matrix_wins": matrix_data_wins,
                "metadata": {
                    "maxWinRate": max_win_rate,
                    "minWinRate": min_win_rate,
                }
            }

            logger.info(f"Successfully computed strength data: {len(matrix_data)} team records")
            return result
        except Exception as format_error:
            log_error_with_context(logger, format_error, {
                "operation": "response_formatting",
                "matrix_shape": df_alt_wins.shape,
                "wins_matrix_shape": df_wins.shape
            })
            raise
    except Exception as e:
        # Log the error with full context
        log_error_with_context(logger, e, {
            "endpoint": "strength",
            "error_type": type(e).__name__
        })
        logger.error(f"Strength calculation failed")
        raise  # Re-raise to let FastAPI handle it


@router.get("/strength")
async def strength(response: Response):
    """Strength endpoint with request coalescing"""
    start_time = datetime.now()
    logger.info("Starting strength calculation endpoint")

    cache_key = "strength"

    try:
        # Use get_or_compute for automatic request coalescing
        result = await cache.get_or_compute(cache_key, _compute_strength)

        # Set cache headers
        cached_value = cache.get(cache_key)
        if cached_value is not None:
            response.headers["X-Cache-Status"] = "HIT"
        else:
            response.headers["X-Cache-Status"] = "MISS"
        response.headers["Cache-Control"] = "public, max-age=300"

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Strength endpoint completed in {duration:.3f}s")

        return result
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.error(f"Strength endpoint failed after {duration:.3f}s: {str(e)}")
        return {"error": str(e)}
