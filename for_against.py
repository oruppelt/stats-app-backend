from fastapi import APIRouter
import pandas as pd
import numpy as np
from constants import SHEET_ID, DATA_TAB
from logger_config import setup_logging, log_data_shape, log_error_with_context
from datetime import datetime

router = APIRouter()
logger = setup_logging("INFO")

@router.get("/for_against")
async def for_against():
    start_time = datetime.now()
    logger.info("Starting for_against calculation endpoint")
    
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

        # Create swapped dataframe
        df_swap = df.copy()
        df_swap.columns = ['Team2 ', 'Score2 ', 'Team1 ', 'Score1 ', 'PERIOD ', 'Match ', 'Result ', 'WINNER ', 'LOSER ']
        logger.info("Created swapped dataframe for bidirectional scoring analysis")
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

        # Group by team to calculate total scores
        try:
            df_scores = df_full.groupby(['Team1 '])[['Score1 ', 'Score2 ']].sum().reset_index()
            logger.info("Successfully grouped scores by team")
            log_data_shape(logger, df_scores, "score_grouping", "team score totals")
        except Exception as group_error:
            log_error_with_context(logger, group_error, {
                "operation": "score_grouping",
                "unique_teams": len(df_full['Team1 '].unique())
            })
            raise

        # Rename columns for clarity
        df_scores.columns = ['Team', 'Scored For', 'Scored Against']
        logger.info("Renamed columns for final output")

        # Calculate statistics
        median_scored_for = np.median(df_scores['Scored For'])
        median_scored_against = np.median(df_scores['Scored Against'])
        logger.info(f"Calculated medians - For: {median_scored_for:.1f}, Against: {median_scored_against:.1f}")

        # Convert to response format
        try:
            scores_list = df_scores.to_dict('records')
            logger.info(f"Successfully formatted response data: {len(scores_list)} team records")
            
            # Log total processing time
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"For/Against calculation completed successfully in {duration:.3f}s")
            
            return {
                "df_scores": scores_list, 
                "median_scored_for": median_scored_for, 
                "median_scored_against": median_scored_against
            }
        except Exception as format_error:
            log_error_with_context(logger, format_error, {
                "operation": "response_formatting",
                "scores_shape": df_scores.shape
            })
            raise
            
    except Exception as e:
        # Log the error with full context
        duration = (datetime.now() - start_time).total_seconds()
        log_error_with_context(logger, e, {
            "endpoint": "for_against",
            "processing_time": f"{duration:.3f}s",
            "error_type": type(e).__name__
        })
        logger.error(f"For/Against calculation failed after {duration:.3f}s")
        return {"error": str(e)}