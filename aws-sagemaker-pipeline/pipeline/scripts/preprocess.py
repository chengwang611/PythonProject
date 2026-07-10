#!/usr/bin/env python3
"""
Preprocessing Script for SageMaker Pipeline

This script is executed by the SKLearnProcessor in the PreprocessData step.
It reads raw CSV data, performs cleaning and feature engineering, and
splits the data into train/validation/test sets.

Input:  /opt/ml/processing/input/  (raw CSV from S3)
Output: /opt/ml/processing/output/train/
        /opt/ml/processing/output/validation/
        /opt/ml/processing/output/test/
"""

import argparse
import os
import json
import logging
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Preprocess data for SageMaker pipeline')
    parser.add_argument('--environment', default='dev', help='Environment name')
    parser.add_argument('--input-path', default='/opt/ml/processing/input', help='Input data path')
    parser.add_argument('--output-path', default='/opt/ml/processing/output', help='Output data path')
    parser.add_argument('--test-size', type=float, default=0.2, help='Test set proportion')
    parser.add_argument('--val-size', type=float, default=0.1, help='Validation set proportion')
    parser.add_argument('--target-column', default='target', help='Target column name')
    parser.add_argument('--random-state', type=int, default=42, help='Random seed')
    return parser.parse_args()


def load_data(input_path: str) -> pd.DataFrame:
    """Load CSV data from the input directory."""
    csv_files = [f for f in os.listdir(input_path) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {input_path}")

    df = pd.read_csv(os.path.join(input_path, csv_files[0]))
    logger.info(f"Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"Target distribution:\n{df.iloc[:, -1].value_counts()}")
    return df


def clean_data(df: pd.DataFrame, target_column: str) -> pd.DataFrame:
    """Clean the data: handle missing values, remove duplicates."""
    initial_rows = len(df)

    # Drop duplicates
    df = df.drop_duplicates()
    logger.info(f"Dropped {initial_rows - len(df)} duplicate rows")

    # Handle missing values
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            if df[col].dtype == 'object':
                df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else 'Unknown', inplace=True)
            else:
                df[col].fillna(df[col].median(), inplace=True)
            logger.info(f"Filled missing values in column: {col}")

    return df


def encode_features(df: pd.DataFrame, target_column: str) -> pd.DataFrame:
    """Encode categorical features and scale numerical features."""
    label_encoders = {}

    for col in df.columns:
        if col == target_column:
            continue
        if df[col].dtype == 'object':
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            label_encoders[col] = le
            logger.info(f"Label encoded column: {col} ({len(le.classes_)} classes)")

    return df


def split_data(df: pd.DataFrame, target_column: str, test_size: float,
               val_size: float, random_state: int):
    """Split data into train, validation, and test sets."""
    X = df.drop(columns=[target_column])
    y = df[target_column]

    # First split: separate test set
    X_temp, X_test, y_temp, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=y
    )

    # Second split: separate validation from remaining
    val_ratio = val_size / (1 - test_size)
    X_train, X_val, y_train, y_val = train_test_split(
        X_temp, y_temp, test_size=val_ratio, random_state=random_state, stratify=y_temp
    )

    logger.info(f"Train: {len(X_train)}, Validation: {len(X_val)}, Test: {len(X_test)}")
    return X_train, X_val, X_test, y_train, y_val, y_test


def save_data(X_train, X_val, X_test, y_train, y_val, y_test, output_path: str):
    """Save split datasets as CSV files."""
    # Combine features and target for XGBoost (target must be first column)
    train_df = pd.concat([y_train, X_train], axis=1)
    val_df = pd.concat([y_val, X_val], axis=1)
    test_df = pd.concat([y_test, X_test], axis=1)

    # Save to output directories
    train_path = os.path.join(output_path, 'train')
    val_path = os.path.join(output_path, 'validation')
    test_path = os.path.join(output_path, 'test')

    os.makedirs(train_path, exist_ok=True)
    os.makedirs(val_path, exist_ok=True)
    os.makedirs(test_path, exist_ok=True)

    train_df.to_csv(os.path.join(train_path, 'train.csv'), index=False, header=False)
    val_df.to_csv(os.path.join(val_path, 'validation.csv'), index=False, header=False)
    test_df.to_csv(os.path.join(test_path, 'test.csv'), index=False, header=True)

    logger.info(f"Saved train: {train_path}/train.csv ({len(train_df)} rows)")
    logger.info(f"Saved validation: {val_path}/validation.csv ({len(val_df)} rows)")
    logger.info(f"Saved test: {test_path}/test.csv ({len(test_df)} rows)")


def main():
    args = parse_args()
    logger.info(f"Starting preprocessing for environment: {args.environment}")

    # Load
    df = load_data(args.input_path)

    # Clean
    df = clean_data(df, args.target_column)

    # Encode
    df = encode_features(df, args.target_column)

    # Split
    X_train, X_val, X_test, y_train, y_val, y_test = split_data(
        df, args.target_column, args.test_size, args.val_size, args.random_state
    )

    # Save
    save_data(X_train, X_val, X_test, y_train, y_val, y_test, args.output_path)

    logger.info("Preprocessing complete!")


if __name__ == '__main__':
    main()
