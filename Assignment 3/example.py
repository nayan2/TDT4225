import datetime
from pprint import pprint
from DbConnector import DbConnector
from bson.objectid import ObjectId
from haversine import haversine, Unit
import os
import glob
import re


class ExampleProgram:

    def __init__(self):
        self.label_user_ids = None
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def truncate_collection(self, collection_name):
        self.db.drop_collection(collection_name)
        print('Collection dropped/truncated - ', collection_name)

    @staticmethod
    def load_file_content(file_path):
        with open(file_path) as file:
            return file.read().splitlines()

    @staticmethod
    def to_iso_date(date_time_string):
        return datetime.datetime.strptime(date_time_string, '%Y-%m-%dT%H:%M:%S')

    def insert_data(self, database_folder_path):
        self.label_user_ids = self.load_file_content(f"{database_folder_path}/labeled_ids.txt")
        user_collection = self.db['User']
        activity_collection = self.db['Activity']
        track_point_collection = self.db['TrackPoint']

        for user in os.scandir(f'{database_folder_path}/Data'):
            is_labelled_user = user.name in self.label_user_ids
            user_obj = {'_id': user.name, 'has_labels': is_labelled_user, 'activities': []}

            # Global variables
            activities_global = []
            track_points_global = []

            # If the user has activity label
            if not is_labelled_user:
                # Loop through the files in that directory
                # For each and every file -> max length validation && find the activity start date and end date
                # Get Track points data set
                # Create an Empty Activity record, and get the id
                # And prepare the dataset and insert

                # Loop through the files in that directory
                for track_point_record in os.scandir(os.path.join(user.path, 'Trajectory')):
                    # For each and every file -> max length validation && find the activity start date and end date
                    # Get Track points data set
                    track_points = self.load_file_content(track_point_record.path)[6:]
                    track_points = track_points[0: 2500] if len(track_points) > 2500 else track_points

                    track_points_start_line = track_points[0].split(',')
                    track_points_end_line = track_points[-1].split(',')

                    # Create a Empty Activity record, and get the id
                    activity_id = ObjectId()
                    user_obj['activities'].append(activity_id)
                    activities_global.append({
                        '_id': activity_id,
                        'user_id': user.name,
                        'transportation_mode': None,
                        'start_date_time': self.to_iso_date(f"{track_points_start_line[-2]}T{track_points_start_line[-1]}"),
                        'end_date_time': self.to_iso_date(f"{track_points_end_line[-2]}T{track_points_end_line[-1]}"),
                        'trackpoints': []
                    })

                    # And prepare the dataset and insert
                    track_points_global = []
                    for track_point in track_points:
                        track_point_data = track_point.split(',')

                        # Create ObjectId and prepare obj
                        track_point_id = ObjectId()
                        activities_global[-1]['trackpoints'].append(track_point_id)
                        track_points_global.append(
                            {
                                '_id': track_point_id,
                                'activity_id': activity_id,
                                'lat': float(track_point_data[0]),
                                'lon': float(track_point_data[1]),
                                'altitude': float(track_point_data[3]),
                                'date_days': float(track_point_data[4]),
                                'date_time': self.to_iso_date(f"{track_point_data[5]}T{track_point_data[6]}")
                            })
            else:
                with open(f"{user.path}/labels.txt") as activity_records:
                    # Don't read the first line(it's the header)
                    # For every activity get the date from the text
                    # Find all the plt files related to that date
                    # Merger them all and remove the unnecessary headers
                    # Check if the activity start date and end date is available in the files, if not do, nothing
                    # If yes check how many, if it's greater than 2500 then do nothing
                    # If all the above condition pass, then prepare the record -> Activity(some issssue witn date and time) to insert into the Activity Table
                    # Then prepare all the data from Trackpoints and merge them all and insert them to TrackPoint table
                    # Done!!!

                    # Don't read the first line(it's the header)
                    for activity_record in activity_records.read().splitlines()[1:]:
                        # For every activity get the date from the text
                        # Format the Date column
                        activity_line_data = re.split('[ \t]', activity_record)
                        activity_line_data[0] = activity_line_data[0].replace('/', '-')
                        activity_line_data[2] = activity_line_data[2].replace('/', '-')

                        # Find all the plt files related to that date
                        track_point_files = glob.glob(
                            f'{os.path.join(user.path, "Trajectory")}/{activity_line_data[0].replace("-", "")}*.plt')
                        # Check if there is any activity with different start date and end date
                        if activity_line_data[0] != activity_line_data[2]:
                            track_point_files.extend(glob.glob(
                                f'{os.path.join(user.path, "Trajectory")}/{activity_line_data[2].replace("-", "")}*.plt'))
                        if len(track_point_files) <= 0: continue  # No files related to the activity found!!

                        # Create activity
                        activity_id = ObjectId()
                        activity = {
                            '_id': activity_id,
                            'user_id': user.name,
                            'transportation_mode': activity_line_data[4],
                            'start_date_time': self.to_iso_date(f"{activity_line_data[0]}T{activity_line_data[1]}"),
                            'end_date_time': self.to_iso_date(f"{activity_line_data[2]}T{activity_line_data[3]}"),
                            'trackpoints': []
                        }

                        # Track points
                        tracking_data = ""
                        track_points_local = []
                        for trackPoint_file in track_point_files:
                            with open(trackPoint_file) as tf:
                                # Merger them all and remove the unnecessary headers
                                tracking_data += ''.join(tf.readlines()[6:])

                        # Check if the activity start date and end date is available in the files, if not do, nothing
                        if (tracking_data.find(f"{activity_line_data[0]},{activity_line_data[1]}") > 0) and (
                                tracking_data.find(f"{activity_line_data[2]},{activity_line_data[3]}") > 0):
                            # If yes check how many, if it's greater than 2500 then do nothing
                            start_point_found = False
                            count = 0
                            for track in tracking_data.splitlines():
                                track_point_data = track.split(',')

                                if start_point_found:
                                    # Insert the data
                                    track_point_id = ObjectId()
                                    activity['trackpoints'].append(track_point_id)
                                    track_points_local.append({
                                        '_id': track_point_id,
                                        'activity_id': activity_id,
                                        'lat': float(track_point_data[0]),
                                        'lon': float(track_point_data[1]),
                                        'altitude': float(track_point_data[3]),
                                        'date_days': float(track_point_data[4]),
                                        'date_time': self.to_iso_date(f"{track_point_data[5]}T{track_point_data[6]}")
                                    })
                                    count += 1

                                    # check for end point also number of count
                                    # End point
                                    if count > 2500 or track.find(f"{activity_line_data[2]},{activity_line_data[3]}") > 0: break
                                elif track.find(f"{activity_line_data[0]},{activity_line_data[1]}") > 0:
                                    start_point_found = True

                        if len(track_points_local) > 0:
                            # If all the above condition pass, then prepare the record ->
                            # Activity(some issue within date and time) to insert into the Activity Table
                            user_obj['activities'].append(activity_id)
                            activities_global.append(activity)
                            track_points_global.extend(track_points_local)

            print(
                f'Complete Processing User - {user.name}. Activity Record count - {len(activities_global)}. Track Point Record Count - {len(track_points_global)}')
            user_collection.insert_one(user_obj)
            if len(activities_global) > 0: activity_collection.insert_many(activities_global)
            if len(track_points_global) > 0: track_point_collection.insert_many(track_points_global)

    def insert_documents(self, collection_name, document=None):
        docs = [] if document is None else document
        collection = self.db[collection_name]
        collection.insert_many(docs)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def query_1(self):
        document = self.db['User'].aggregate([
            {
                '$count': 'TotalUser'
            },
            {
                '$lookup': {
                    'from': "Activity",
                    'pipeline': [{ '$count': "TotalActivity" }],
                    'as': "Activity"
                }
            },
            {
                '$lookup': {
                    'from': "TrackPoint",
                    'pipeline': [{'$count': "TotalTrackPoint"}],
                    'as': "TrackPoint"
                }
            },
            {
                '$project': {
                    'TotalUser': 1,
                    'TotalActivity': { '$arrayElemAt': [ "$Activity.TotalActivity", 0] },
                    'TotalTrackPoint': { '$arrayElemAt': [ "$TrackPoint.TotalTrackPoint", 0] }
                }
            }
        ])
        pprint(list(document))

    def query_2(self):
        document = self.db['User'].aggregate([{
            '$project': {
                '_id': 0,
                'userId': '$_id',
                'activityCount': { '$size': '$activities' }
            }
        }])
        pprint(list(document))

    def query_3(self):
        document = self.db['User'].aggregate([
            {
                '$project': {
                    '_id': 0,
                    'userId': '$_id',
                    'activityCount': {'$size': '$activities'}
                }
            },
            {
                '$sort': {
                    'activityCount': -1
                }
            },
            {
                '$limit': 20
            }
        ])
        pprint(list(document))

    def query_4(self):
        document = self.db['Activity'].aggregate([
            {
                '$match': { 'transportation_mode': 'taxi' }
            },
            {
                '$group': { "_id": { 'userId': "$user_id", 'transportationMode': "$transportation_mode" } }
            },
            {
                '$project': {
                    '_id': 0,
                    'userId': '$_id.userId',
                    'transportationMode': '$_id.transportationMode'
                }
            }
        ])
        pprint(list(document))

    def query_5(self):
        document = self.db['Activity'].aggregate([
            {
                '$match': {'transportation_mode': { '$ne': None }}
            },
            {
                '$group': {
                    '_id': '$transportation_mode',
                    'count': { '$sum': 1 }
                }
            },
            {
                '$lookup': {
                    'from': 'TrackPoint',
                    'localField': "_id",
                    'foreignField': "activity_id",
                    'as': "TrackPoints"
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'transportation_mode': '$_id',
                    'count': '$count'
                }
            }
        ])
        pprint(list(document))

    def query_6(self):
        document1 = self.db['Activity'].aggregate([
            {
                '$group': {
                    '_id': { '$year': '$start_date_time'},
                    'activityCount': { '$sum': 1}
                }
            },
            {
                '$sort': { 'activityCount': -1 }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    '_id': 0,
                    'year': '$_id',
                    'activityCount': '$activityCount'
                }
            }
        ])

        document2 = self.db['Activity'].aggregate([
            {
                '$group': {
                    '_id': { '$year': '$start_date_time'},
                    'hoursCount': { '$sum': { '$dateDiff': {'startDate': "$start_date_time", 'endDate': "$end_date_time", 'unit': "hour"}}}
                }
            },
            {
                '$sort': {
                    'hoursCount': -1
                }
            },
            {
                '$limit': 1
            },
            {
                '$project': {
                    '_id': 0,
                    'year': '$_id',
                    'hoursCount': '$hoursCount'
                }
            }
        ])

        document1 = list(document1)
        document2 = list(document2)

        pprint(document1)
        pprint(document2)
        print(f'Are the year same - {document1[0]["year"] == document2[0]["year"]}')

    def query_7(self):
        document = self.db['Activity'].aggregate([
            {
                '$project': {
                    'transportation_mode': '$transportation_mode',
                    'user_id': '$user_id',
                    'start_year': { '$year': '$start_date_time'}
                }
            },
            {
                '$match': {
                    '$and': [
                        {'transportation_mode': 'walk'},
                        {'start_year': 2008},
                        {'user_id': "112"}
                    ]
                }
            },
            {
                '$lookup': {
                    'from': 'TrackPoint',
                    'localField': "_id",
                    'foreignField': "activity_id",
                    'as': "TrackPoints"
                }
            },
            {
                '$unwind': '$TrackPoints'
            },
            {
                '$project': {
                    '_id': 0,
                    'lat': '$TrackPoints.lat',
                    'lon': '$TrackPoints.lon'
                }
            }
        ])
        document = list(document)

        # Loop through all the record and find the distance using haversine
        distance = []
        records_len = len(document)
        for idx, record in enumerate(document):
            if idx + 1 != records_len:
                distance.append(haversine(tuple(record.values()), tuple(document[idx + 1].values()), unit=Unit.KILOMETERS))
        # Sum them up and walla! the result
        print(f'Total Distance walked by user -> 112 is - {sum(distance)} KM')

    def query_8(self):
        document = self.db['TrackPoint'].aggregate([
            {
                '$match': {'altitude': {'$gt': 0.0}}
            },
            {
                '$setWindowFields': {
                    'partitionBy': None,
                    'sortBy': {
                        'date_time': 1
                    },
                    'output': {
                        'altitudes': {
                            '$addToSet': "$altitude",
                            'window': {
                                'documents': [-1, 0]
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    '$expr': {
                        '$and': [
                            {'$eq': [{'$size': '$altitudes'}, 2]},
                            {'$gt': [{'$arrayElemAt': ['$altitudes', 1]}, {'$arrayElemAt': ['$altitudes', 0]}]}
                        ]
                    }
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': "activity_id",
                    'foreignField': "_id",
                    'as': "Activity"
                }
            },
            {
                '$unwind': '$Activity'
            },
            {
                '$group': {
                    '_id': '$Activity.user_id',
                    'maxAltitude': {'$max': '$altitude'}
                }
            },
            {
                '$sort': {
                    'maxAltitude': -1
                }
            },
            {
                '$limit': 20
            },
            {
                '$project': {
                    '_id': 0,
                    'userId': '$_id',
                    'maxAltitude': '$maxAltitude'
                }
            }
        ])
        pprint(list(document))

    def query_9(self):
        document = self.db['TrackPoint'].aggregate([
            {
                '$setWindowFields': {
                    'partitionBy': None,
                    'sortBy': {
                        'date_time': 1
                    },
                    'output': {
                        'tp_datetimes': {
                            '$addToSet': "$date_time",
                            'window': {
                                'documents': [-1, 0]
                            }
                        }
                    }
                }
            },
            {
                '$match': {
                    '$expr': {
                        '$and': [
                            {'$eq': [{'$size': '$tp_datetimes'}, 2]},
                            {'$gte': [{'$dateDiff': {'startDate': {'$arrayElemAt': ['$tp_datetimes', 0]},
                                                     'endDate': {'$arrayElemAt': ['$tp_datetimes', 1]},
                                                     'unit': "minute"}}, 5]}
                        ]
                    }
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': "activity_id",
                    'foreignField': "_id",
                    'as': "Activity"
                }
            },
            {
                '$unwind': '$Activity'
            },
            {
                '$group': {
                    '_id': '$Activity.user_id',
                    'count': {'$sum': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'userId': '$_id',
                    'count': '$count'
                }
            }
        ])
        pprint(list(document))

    def query_10(self):
        document = self.db['TrackPoint'].aggregate([
            {
                '$match': {
                    '$and': [
                        {'lat': 39.916},
                        {'lon': 116.397}
                    ]
                }
            },
            {
                '$lookup': {
                    'from': 'Activity',
                    'localField': "activity_id",
                    'foreignField': "_id",
                    'as': "Activity"
                }
            },
            {
                '$project': {'_id': 0, 'user_id': '$Activity.user_id'}
            },
            {
                '$unwind': '$user_id'
            },
            {
                '$group': {'_id': '$user_id'}
            }
        ])
        pprint(list(document))

    def query_11(self):
        document = self.db['Activity'].aggregate([
            {
                '$match': {
                    'transportation_mode': {'$ne': None}
                }
            },
            {
                '$group': {
                    '_id': {'userId': '$user_id', 'transportation_mode': '$transportation_mode'},
                    'count': {'$sum': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'userId': '$_id.userId',
                    'transportation_mode': '$_id.transportation_mode',
                    'count': '$count',
                    'transportation_mode_len': {'$strLenCP': '$_id.transportation_mode'}
                }
            },
            {
                '$sort': {
                    'userId': 1,
                    'count': -1,
                    'transportation_mode_len': -1
                }
            },
            {
                '$group': {
                    '_id': "$userId",
                    'most_used_transportation_mode': {
                        '$firstN': {
                            'input': "$transportation_mode",
                            'n': 1
                        }
                    }
                }
            },
            {
                '$unwind': '$most_used_transportation_mode'
            },
            {
                '$sort': {
                    '_id': 1
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'user_id': '$_id',
                    'most_used_transportation_mode': '$most_used_transportation_mode'
                }
            }
        ])
        pprint(list(document))


def main():
    program = None
    try:
        # Initialize the program and collect with Mongo DB
        program = ExampleProgram()

        # create the collections
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="TrackPoint")

        # Load the documents to collection
        program.insert_data('dataset')

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
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
