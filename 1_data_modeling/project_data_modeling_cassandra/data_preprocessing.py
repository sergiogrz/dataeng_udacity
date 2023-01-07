#!/usr/bin/env python

import os
from glob import glob
import csv


def main():
    # Create list of filepaths to process original event csv data files
    path = os.getcwd() + "/event_data"
    for root, dirs, files in os.walk(path):
        files_path_list = glob(os.path.join(root, "*"))

    # Process the files to create a unique data file csv to be used for
    # Apache Cassandra data modeling
    full_data_rows_list = []
    for file_path in files_path_list:
        # Read csv file
        with open(file_path, "r", encoding="utf8", newline="") as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)  # ignore the header

            # Extract each data row one by one and append it the the list
            for line in csvreader:
                full_data_rows_list.append(line)

    # Create a smaller event data csv file called event_datafile.csv that will
    # be used to insert data into the Apache Cassandra tables
    csv.register_dialect("myDialect", quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open("event_datafile.csv", "w", encoding="utf8", newline="") as f:
        writer = csv.writer(f, dialect="myDialect")
        writer.writerow(
            [
                "artist_name",
                "user_first_name",
                "user_gender",
                "item_in_session",
                "user_last_name",
                "song_length",
                "level",
                "user_location",
                "session_id",
                "song_title",
                "user_id",
            ]
        )
        for row in full_data_rows_list:
            if row[0] == "":
                continue
            writer.writerow(
                (
                    row[0],
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[12],
                    row[13],
                    row[16],
                )
            )


if __name__ == "__main__":
    main()
