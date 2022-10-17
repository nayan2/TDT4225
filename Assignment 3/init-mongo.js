db = new Mongo().getDB("TDT4225");

db.createCollection('User', { capped: false });
db.createCollection('Activity', { capped: false });
db.createCollection('TrackPoint', { capped: false });