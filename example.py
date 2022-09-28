import queue
from unicodedata import decimal
from haversine import haversine, Unit
from DbConnector import DbConnector
from tabulate import tabulate
import os
import glob
import re
import sys

class UserActivity:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.label_user_ids = []

    def create_table(self, table_name):
        tableSQl = {
            "User": """CREATE TABLE IF NOT EXISTS User (
                        id VARCHAR(3) PRIMARY KEY,
                        has_labels BOOLEAN NOT NULL
                    );
            """,
            "Activity": """CREATE TABLE IF NOT EXISTS Activity (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            user_id VARCHAR(3) NOT NULL,
                            transportation_mode VARCHAR(50),
                            start_date_time DATETIME NOT NULL,
                            end_date_time DATETIME NOT NULL,
                            FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE ON UPDATE NO ACTION
                        );
            """,
            "TrackPoint": """CREATE TABLE IF NOT EXISTS TrackPoint (
                                id INT PRIMARY KEY AUTO_INCREMENT,
                                activity_id INT NOT NULL,
                                lat DOUBLE NOT NULL,
                                lon DOUBLE NOT NULL,
                                altitude DOUBLE NOT NULL,
                                date_days DOUBLE NOT NULL,
                                date_time DATETIME NOT NULL,
                                FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE ON UPDATE NO ACTION
                            );
            """
        }

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(tableSQl[table_name])
        self.db_connection.commit()
        
    def truncate_table(self, table_name):
        truncate_tableSQl = {
            "User": "DELETE FROM User;",
            "Activity": "DELETE FROM Activity;",
            "TrackPoint": "DELETE FROM TrackPoint;"
        }
        
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(truncate_tableSQl[table_name])
        self.db_connection.commit()

    def load_file_content(self, file_path):
        with open(file_path) as file:
            return file.read().splitlines()

    def insert_data(self, database_folder_path):
        self.label_user_ids = self.load_file_content(f"{database_folder_path}/labeled_ids.txt")
        user_insert_query = """INSERT INTO User (id, has_labels) VALUES (%s, %s)"""
        activity_insert_query = """INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"""
        trackPoint_insert_query = """INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"""
        
        users_with_no_label = []
        for user in os.scandir(f'{database_folder_path}/Data'):
            print('Processing User - %', user.name)
            
            is_lebelled_user = user.name in self.label_user_ids
            self.cursor.execute(user_insert_query, (user.name, is_lebelled_user))
            
            # If the user has activity lebel
            if not is_lebelled_user:
                # Loop through the files in that directory
                # For each and every file -> max length validation && find the activity start date and end date
                # Get Track points data set
                # Create a Empty Activity record, and get the Id
                # And prepare the dataset and insert
                
                # Loop through the files in that directory
                for trackpoint_record in os.scandir(os.path.join(user.path, 'Trajectory')):
                    # For each and every file -> max length validation && find the activity start date and end date
                    # Get Track points data set
                    track_points = self.load_file_content(trackpoint_record.path)[6:]
                    if len(track_points) > 2500: continue
                    
                    track_points_start_line = track_points[0].split(',')
                    track_points_end_line = track_points[-1].split(',')
                    
                    # Create a Empty Activity record, and get the Id
                    self.cursor.execute(activity_insert_query, (user.name, None, f"{track_points_start_line[-2]} {track_points_start_line[-1]}", f"{track_points_end_line[-2]} {track_points_end_line[-1]}"))
                    activity_id = self.cursor.lastrowid
                    
                    # And prepare the dataset and insert
                    trackPoints = []
                    for track_point in track_points:
                        track_point_data = track_point.split(',')
                        trackPoints.append((activity_id, float(track_point_data[0]), float(track_point_data[1]), float(track_point_data[3]), float(track_point_data[4]), f"{track_point_data[5]} {track_point_data[6]}"))
                        
                    self.cursor.executemany(trackPoint_insert_query, trackPoints)
            else:
                with open(f"{user.path}/labels.txt") as activities:
                    # Dont read the first line(its the header)
                    # For every activity get the date from the text
                    # Find all the plt files related to that date
                    # Merger them all and remove the unnecessary headers
                    # Check if the activity start date and end date is available in the files, if not do nothing
                    # If yes check how many, if its greater than 2500 then do nothing
                    # If all the above condition pass, then prepare the record -> Activity(some issssue witn date and time) to insert into the Activity Table
                    # Then prepare all the data from Trackpoints and merge them all and insert them to TrackPoint table
                    # Done!!!
                    
                    for activity in activities.read().splitlines()[1:]: # Dont read the first line(its the header)
                        # For every activity get the date from the text
                        # Format the Date column
                        activity_line_data = re.split(' |\\t', activity)
                        activity_line_data[0] = activity_line_data[0].replace('/', '-')
                        activity_line_data[2] = activity_line_data[2].replace('/', '-')

                        # Find all the plt files related to that date
                        trackPoint_files = glob.glob(f'{os.path.join(user.path, "Trajectory")}/{activity_line_data[0].replace("-", "")}*.plt')
                        # Check if there any activity with different start date and end date
                        if activity_line_data[0] != activity_line_data[2]:
                            trackPoint_files.extend(glob.glob(f'{os.path.join(user.path, "Trajectory")}/{activity_line_data[2].replace("-", "")}*.plt'))
                        if len(trackPoint_files) <= 0: continue # No files related to the activity found!!
                        
                        tracking_data = ""
                        trackPoints = []
                        for trackPoint_file in trackPoint_files:
                            with open(trackPoint_file) as tf:
                                # Merger them all and remove the unnecessary headers
                                tracking_data += ''.join(tf.readlines()[6:])
                                
                        # Check if the activity start date and end date is available in the files, if not do nothing
                        if (tracking_data.find(f"{activity_line_data[0]},{activity_line_data[1]}") > 0) and (tracking_data.find(f"{activity_line_data[2]},{activity_line_data[3]}") > 0):
                            # If yes check how many, if its greater than 2500 then do nothing
                            start_point_found = False
                            count = 0
                            for track in tracking_data.splitlines():
                                track_point_data = track.split(',')
                                if not start_point_found:
                                    if track.find(f"{activity_line_data[0]},{activity_line_data[1]}") > 0:
                                        start_point_found = True
                                        trackPoints.append([0, float(track_point_data[0]), float(track_point_data[1]), float(track_point_data[3]), float(track_point_data[4]), f"{track_point_data[5]} {track_point_data[6]}"])
                                        count += 1
                                else:
                                    # check for end point also number of count
                                    if (count > 2500):
                                        trackPoints = []
                                        break
                                    
                                    trackPoints.append([0, float(track_point_data[0]), float(track_point_data[1]), float(track_point_data[3]) if float(track_point_data[3]) > 0 else 0, float(track_point_data[4]), f"{track_point_data[5]} {track_point_data[6]}"])
                                    count += 1
                                    
                                    # End point
                                    if track.find(f"{activity_line_data[2]},{activity_line_data[3]}") > 0: break
                        
                        if len(trackPoints) > 0:
                            # If all the above condition pass, then prepare the record -> Activity(some issssue witn date and time) to insert into the Activity Table
                            self.cursor.execute(activity_insert_query, (user.name, activity_line_data[4], f"{activity_line_data[0]} {activity_line_data[1]}", f"{activity_line_data[2]} {activity_line_data[3]}"))
                            activity_id = self.cursor.lastrowid
                            for trackpoint in trackPoints: trackpoint[0] = activity_id
                            
                            self.cursor.executemany(trackPoint_insert_query, list(map(tuple, trackPoints)))

        # Insert all users with no label
        self.cursor.executemany(user_insert_query, users_with_no_label)
        self.db_connection.commit()
        
    def fetch_data(self, query):
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows
        
    def query_1(self):
        query = """SELECT
                    (SELECT COUNT(*) FROM USER) as UserCount,
                    (SELECT COUNT(*) FROM ACTIVITY) as ActivityCount,
                    (SELECT COUNT(*) FROM TRACKPOINT) as TrackPointCount
                """
        self.fetch_data(query)
    
    def query_2(self):
        query = """SELECT
                        U.ID,
                        COUNT(A.ID)
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    GROUP BY U.ID;
                """
        self.fetch_data(query)
    
    def query_3(self):
        query = """SELECT
                        U.ID,
                        COUNT(A.ID)
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    GROUP BY U.ID
                    ORDER BY COUNT(A.ID) DESC LIMIT 20;
                """
        self.fetch_data(query)

    def query_4(self):
        query = """SELECT
                        DISTINCT U.ID, A.TRANSPORTATION_MODE
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    WHERE A.TRANSPORTATION_MODE = 'taxi'
                """
        self.fetch_data(query)

    def query_5(self):
        query = """SELECT
                        A.TRANSPORTATION_MODE,
                        COUNT(TP.ID)
                    FROM ACTIVITY A
                    INNER JOIN TRACKPOINT TP ON A.ID = TP.ACTIVITY_ID
                    WHERE A.TRANSPORTATION_MODE IS NOT NULL
                    GROUP BY A.TRANSPORTATION_MODE
                """
        self.fetch_data(query)
    
    def query_6(self):
        query_one = """SELECT
                            YEAR(A.START_DATE_TIME) Year,
                            COUNT(A.START_DATE_TIME) ActivityCount
                        FROM ACTIVITY A
                        GROUP BY YEAR(A.START_DATE_TIME)
                        ORDER BY COUNT(A.START_DATE_TIME) DESC LIMIT 1;
                    """
        query_two = """SELECT
                            YEAR(A.START_DATE_TIME) Year,
                            SUM(TIMESTAMPDIFF(HOUR, A.START_DATE_TIME, A.END_DATE_TIME)) HoursCount
                        FROM ACTIVITY A
                        GROUP BY YEAR(A.START_DATE_TIME)
                        ORDER BY SUM(TIMESTAMPDIFF(HOUR, A.START_DATE_TIME, A.END_DATE_TIME)) DESC LIMIT 1;
                    """
        query_three = """SELECT
                            YearWithActivityCount.YEAR = YearWithHoursCount.YEAR
                        FROM
                            (
                                SELECT
                                    YEAR(A.START_DATE_TIME) Year,
                                    COUNT(A.START_DATE_TIME) ActivityCount
                                FROM ACTIVITY A
                                GROUP BY YEAR(A.START_DATE_TIME)
                                ORDER BY COUNT(A.START_DATE_TIME) DESC LIMIT 1
                        ) AS YearWithActivityCount,
                        (
                                SELECT
                                    YEAR(A.START_DATE_TIME) Year,
                                    SUM(TIMESTAMPDIFF(HOUR, A.START_DATE_TIME, A.END_DATE_TIME)) HoursCount
                                FROM ACTIVITY A
                                GROUP BY YEAR(A.START_DATE_TIME)
                                ORDER BY SUM(TIMESTAMPDIFF(HOUR, A.START_DATE_TIME, A.END_DATE_TIME)) DESC LIMIT 1
                        ) AS YearWithHoursCount
                    """
        self.fetch_data(query_one)
        self.fetch_data(query_two)
        self.fetch_data(query_three)

    def query_7(self):
        query = """SELECT
                        TP.LAT,
                        TP.LON
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    INNER JOIN TRACKPOINT TP ON A.ID = TP.ACTIVITY_ID
                    WHERE U.ID = '112' AND A.TRANSPORTATION_MODE = 'walk' AND YEAR(A.START_DATE_TIME) = 2008
                """
        # Retrieve the table
        records = self.fetch_data(query)
        # Loop through all the record and and find the distance using haversine
        distance = []
        records_len = len(records)
        for idx, record in enumerate(records):
            if idx + 1 != records_len:
                distance.append(haversine(record, records[idx+1], unit=Unit.KILOMETERS))
        # Sum them up and walla! the result
        print(f'Total Distance walked by user -> 112 is - {sum(distance)} KM')

    def query_8(self):
        query = """SELECT
                        A.USER_ID AS user_id,
                        MAX(TP.ALTITUDE) AS total_meters_gained
                    FROM ACTIVITY A
                    INNER JOIN TRACKPOINT TP ON A.ID = TP.ACTIVITY_ID
                    INNER JOIN TRACKPOINT TP2 ON TP2.ACTIVITY_ID = TP.ACTIVITY_ID AND TP2.ID = TP.ID + 1
                    WHERE TP.ALTITUDE > 0 AND TP.ALTITUDE < TP2.ALTITUDE
                    GROUP BY A.USER_ID ORDER BY MAX(TP.ALTITUDE) DESC LIMIT 20
                """
        self.fetch_data(query)
    
    def query_9(self):
        query = """SELECT
                        U.ID,
                        COUNT(TP.ID)
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    INNER JOIN TRACKPOINT TP ON A.ID = TP.ACTIVITY_ID
                    INNER JOIN TrackPoint TP2 ON TP2.ID = TP.ID + 1
                    WHERE TIMESTAMPDIFF(MINUTE, TP.DATE_TIME, TP2.DATE_TIME) >= 5
                    GROUP BY U.ID
                """
        self.fetch_data(query)

    def query_10(self):
        query = """SELECT
                        DISTINCT U.ID
                    FROM USER U
                    INNER JOIN ACTIVITY A ON U.ID = A.USER_ID
                    INNER JOIN TRACKPOINT TP ON A.ID = TP.ACTIVITY_ID
                    WHERE TP.LAT = 39.916 AND TP.LON = 116.397
                """
        self.fetch_data(query)

    def query_11(self):
        query = """ CREATE TABLE IF NOT EXISTS TempUserTransport (ID INT, UserId VARCHAR(3), Transportation_mode VARCHAR(50), Occurrence INT);
                    TRUNCATE TABLE TempUserTransport;

                    SET @rowid = 0;
                    INSERT INTO TempUserTransport
                    SELECT
                        @rowid:=@rowid+1 as Id,
                        A.USER_ID AS UserId,
                        A.TRANSPORTATION_MODE AS Transportation_mode,
                        COUNT(A.ID) Occurrence
                    FROM ACTIVITY A
                    WHERE A.TRANSPORTATION_MODE IS NOT NULL
                    GROUP BY A.USER_ID, A.TRANSPORTATION_MODE
                    ORDER BY A.USER_ID ASC, COUNT(A.ID) DESC, LENGTH(A.TRANSPORTATION_MODE) DESC;


                    SELECT
                        USERID AS user_id,
                        TRANSPORTATION_MODE AS most_used_transportation_mode
                    FROM TEMPUSERTRANSPORT WHERE ID IN (
                        SELECT MIN(TT.ID) FROM TEMPUSERTRANSPORT TT GROUP BY TT.USERID
                    );
                """
        self.fetch_data(query)

def main():
    program = None
    try:
        program = UserActivity()
        # program.truncate_table('User')
        # program.insert_data('dataset\dataset')
        program.query_1()
        program.query_2()
        program.query_3()
        program.query_4()
        program.query_5()
        program.query_6()
        program.query_7()
        program.query_8()
        program.query_9()
        program.query_10()
        program.query_11()
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno

        print("Exception type: ", exception_type)
        print("File name: ", filename)
        print("Line number: ", line_number)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
