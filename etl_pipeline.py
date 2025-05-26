import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import logging

# Configure logging for better insights into the pipeline execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SalariesETLPipeline:
    """
    A class to encapsulate the ETL process (Extract, Transform, Load)
    specifically tailored for the Salaries.csv dataset.
    """
    def __init__(self, input_filepath, output_filepath):
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath
        self.data = None
        self.preprocessor = None
        self.numerical_features = []
        self.categorical_features = []
        self.columns_to_drop = ['Notes', 'Status'] # Columns identified as empty or irrelevant from initial inspection
        # Columns that should be kept as is and not transformed by ColumnTransformer
        self.pass_through_untransformed_cols = ['Id', 'EmployeeName']

    def extract_data(self):
        """
        Extracts data from the specified input filepath (CSV).
        """
        logging.info(f"Extracting data from {self.input_filepath}...")
        try:
            self.data = pd.read_csv(self.input_filepath)
            logging.info(f"Data extracted successfully. Initial shape: {self.data.shape}")
            logging.info("\n--- Initial Data Info ---")
            self.data.info()
            logging.info("\n--- Initial Missing Values ---")
            logging.info(self.data.isnull().sum())
        except FileNotFoundError:
            logging.error(f"Error: Input file not found at {self.input_filepath}")
            raise
        except Exception as e:
            logging.error(f"An error occurred during data extraction: {e}")
            raise

    def clean_and_define_features(self):
        """
        Cleans the data (e.g., dropping irrelevant columns, converting types)
        and identifies numerical and categorical features.
        """
        if self.data is None:
            logging.error("Data not loaded. Call extract_data() first.")
            return

        logging.info("Cleaning data and defining features...")

        # Drop identified irrelevant columns
        original_columns = set(self.data.columns)
        self.data = self.data.drop(columns=[col for col in self.columns_to_drop if col in self.data.columns], errors='ignore')
        dropped_columns = original_columns - set(self.data.columns)
        if dropped_columns:
            logging.info(f"Dropped predefined irrelevant columns: {', '.join(dropped_columns)}")
        else:
            logging.info("No predefined irrelevant columns dropped or columns not found.")

        # Convert potential numeric columns that might be objects due to initial parsing issues
        # (e.g., empty strings or non-numeric chars)
        potential_numeric_cols = ['BasePay', 'OvertimePay', 'OtherPay', 'Benefits', 'TotalPay', 'TotalPayBenefits', 'Year']
        for col in potential_numeric_cols:
            if col in self.data.columns:
                # Ensure the column exists before attempting conversion
                # pd.to_numeric handles non-numeric values by coercing to NaN
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                logging.info(f"Converted '{col}' column to numeric (coercing errors to NaN).")

        # Define features for transformation. Exclude explicit passthrough columns from these lists.
        all_cols_after_initial_drop = [col for col in self.data.columns if col not in self.pass_through_untransformed_cols]

        self.numerical_features = self.data[all_cols_after_initial_drop].select_dtypes(include=np.number).columns.tolist()
        self.categorical_features = self.data[all_cols_after_initial_drop].select_dtypes(include='object').columns.tolist()

        logging.info(f"Identified Numerical Features for transformation: {self.numerical_features}")
        logging.info(f"Identified Categorical Features for transformation: {self.categorical_features}")
        logging.info(f"Columns to pass through untransformed: {self.pass_through_untransformed_cols}")


    def build_preprocessing_pipeline(self):
        """
        Builds a Scikit-learn preprocessing pipeline using ColumnTransformer.
        Remainder is set to 'drop' because we manually handle passthrough columns.
        """
        if not self.numerical_features and not self.categorical_features:
            logging.error("No features defined for transformation. Call clean_and_define_features() first.")
            return

        # Preprocessing for numerical features: impute with median, then scale
        numerical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')), # Median is robust to outliers
            ('scaler', StandardScaler())
        ])

        # Preprocessing for categorical features: impute with most frequent, then one-hot encode
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore')) # Handles new categories in future data
        ])

        # Create a preprocessor using ColumnTransformer.
        # We explicitly set remainder='drop' because we are manually handling 'Id' and 'EmployeeName'.
        # This prevents ColumnTransformer from attempting to process these columns itself.
        self.preprocessor = ColumnTransformer(
            transformers=[
                ('num', numerical_transformer, self.numerical_features),
                ('cat', categorical_transformer, self.categorical_features)
            ],
            remainder='drop' # Explicitly drop any columns not specified in transformers
        )
        logging.info("Preprocessing pipeline built successfully with remainder='drop'.")

    def transform_data(self):
        """
        Applies the built preprocessing pipeline to the data.
        Manually handles columns that should not be transformed by ColumnTransformer.
        """
        if self.data is None:
            logging.error("Data not loaded. Call extract_data() first.")
            return
        if self.preprocessor is None:
            logging.error("Preprocessing pipeline not built. Call build_preprocessing_pipeline() first.")
            return

        logging.info("Transforming data...")

        # Separate the columns that will be processed by the ColumnTransformer
        # and those that will be passed through without transformation.
        
        # Make a copy of the columns to pass through to avoid SettingWithCopyWarning
        passthrough_df = self.data[self.pass_through_untransformed_cols].copy()

        # Create the DataFrame containing only the features for ColumnTransformer to process
        features_df = self.data[self.numerical_features + self.categorical_features].copy()
        
        logging.info(f"Shape of features_df before transformation: {features_df.shape}")
        logging.info(f"Columns in features_df: {features_df.columns.tolist()}")

        # Apply transformation using the preprocessor
        transformed_features_array = self.preprocessor.fit_transform(features_df)

        # Ensure the output is a dense array before creating DataFrame
        # ColumnTransformer can return sparse matrices, especially if OneHotEncoder is used
        if hasattr(transformed_features_array, 'toarray'):
            transformed_features_array = transformed_features_array.toarray()
            logging.info("Converted sparse matrix output from ColumnTransformer to dense array.")

        logging.info(f"Shape of transformed_features_array: {transformed_features_array.shape}")
        
        # Get feature names after transformation (these will be for numerical and one-hot encoded categorical features)
        transformed_feature_names = self.preprocessor.get_feature_names_out()
        logging.info(f"Number of transformed_feature_names: {len(transformed_feature_names)}")

        # Create a DataFrame from the transformed features
        transformed_features_df = pd.DataFrame(
            transformed_features_array,
            columns=transformed_feature_names,
            index=features_df.index # Keep original index for alignment
        )

        # Concatenate the manually passed-through columns with the transformed features
        # Ensure indices align for correct concatenation
        # Using .copy() to ensure independent dataframes and avoid SettingWithCopyWarning
        self.data = pd.concat([passthrough_df.reset_index(drop=True), transformed_features_df.reset_index(drop=True)], axis=1)

        logging.info(f"Data transformed successfully. New shape: {self.data.shape}")
        logging.info("\n--- Sample of Transformed Data ---")
        logging.info(self.data.head())
        logging.info("\n--- Transformed Data Info ---")
        self.data.info()
        logging.info("\n--- Missing values in Transformed Data ---")
        logging.info(self.data.isnull().sum())

    def load_data(self):
        """
        Loads the processed data to the specified output filepath (e.g., CSV).
        """
        if self.data is None:
            logging.error("No data to load. Call transform_data() first.")
            return

        logging.info(f"Loading processed data to {self.output_filepath}...")
        try:
            self.data.to_csv(self.output_filepath, index=False)
            logging.info("Processed data loaded successfully.")
        except Exception as e:
            logging.error(f"An error occurred during data loading: {e}")
            raise

    def run_pipeline(self):
        """
        Executes the full ETL pipeline.
        """
        logging.info("Starting ETL pipeline for Salaries.csv...")
        self.extract_data()
        self.clean_and_define_features()
        self.build_preprocessing_pipeline()
        self.transform_data()
        self.load_data()
        logging.info("ETL pipeline completed.")

# --- Main execution ---
if __name__ == "__main__":
    INPUT_FILE = 'Salaries.csv'
    OUTPUT_FILE = 'processed_salaries_data.csv'

    # Create an instance of the ETL pipeline and run it
    pipeline = SalariesETLPipeline(
        input_filepath=INPUT_FILE,
        output_filepath=OUTPUT_FILE
    )
    pipeline.run_pipeline()

    # Optional: Verify the processed data by loading it
    try:
        processed_df = pd.read_csv(OUTPUT_FILE)
        logging.info(f"\n--- Verification: First 5 rows of {OUTPUT_FILE} ---")
        logging.info(processed_df.head())
        logging.info(f"\n--- Verification: Info of {OUTPUT_FILE} ---")
        processed_df.info()
    except Exception as e:
        logging.error(f"Could not load or display processed data for verification: {e}")