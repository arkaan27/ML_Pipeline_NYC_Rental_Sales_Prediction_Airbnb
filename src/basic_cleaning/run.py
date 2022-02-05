#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    # Crating a run instance
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    # Downloading the artifact and logging
    logger.info("Downloading the artifact")
    artifact_local_path = wandb.use_artifact(args.input_artifact).file()

    # Creating dataframe
    logger.info("Creating the dataframe")
    df = pd.read_csv(artifact_local_path)

    # Fixing the data
    logger.info("Dropping Outliers")
    idx= df['price'].between(args.min_price,args.max_price)

    # Copying dataframe
    logger.info("Copying dataframe")
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting Last review to datetime")
    df['last_review']= pd.to_datetime(df['last_review'])

    # Uploading Artifact
    logger.info("Uploading the artifact")
    df.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )

    artifact.add_file("clean_sample.csv")


    #Logging artifact
    logger.info("Logging the artifact")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualifed name for the input artifact",
        required=True,
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the artifact name to be outputted",
        required=True,
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of artifact to be outputted",
        required=True,
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help= "Minimum price for the dataset",
        required=True,
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price for the dataset",
        required=True,
    )


    args = parser.parse_args()

    go(args)
