import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
import datetime as dt


# Setting up the Engine
######################
path = '/Users/noahsuskin/desktop/RU-JER-DATA-PT-04-2020-U-C/Homework/10-Advanced-Data-Storage-and-Retrieval/Instructions/Resources/hawaii.sqlite'
engine = create_engine(f"sqlite:///{path}")
# ----------
Base = automap_base()
Base.prepare(engine, reflect=True)
# ----------
measurement = Base.classes.measurement
station = Base.classes.station
# ----------
conn = engine.connect()
######################

#############################################################
# Extracting the dates

sessions = Session(engine)
max_date = sessions.query(func.max(measurement.date)).all()
#
start_date_lst = max_date[0][0].split('-')
start_date_year = start_date_lst[0]

start_date_month = start_date_lst[1]
if start_date_month[0] == '0':
    start_date_month = start_date_month.replace('0', '')


start_date_day = start_date_lst[2]
if start_date_day[0] == '0':
    start_date_day = start_date_day.replace('0', '')
##############################################################

start_date = dt.date(int(start_date_year), int(start_date_month), int(start_date_day)) - dt.timedelta(days=365)


# Creating the app in Flask
app = Flask(__name__)


# App Routes
@app.route('/')
def welcome():
    return_string = f'Here is a list of all the available API Routes:<br/>'\
                    f'<br/>'\
                    f'/api/v1.0/precipitation <br/>' \
                    f'/api/v1.0/stations <br/>' \
                    f'/api/v1.0/tobs <br/>' \
                    f'<br/>'\
                    f'Please replace "start_date" and "end_date" with your desired start ' \
                    f'and end dates in this format "YYYY-MM-DD" <br/>'\
                    f'<br/>'\
                    f'/api/v1.0/start_date <br/>' \
                    f'/api/v1.0/start_date/end_date'

    return return_string


@app.route('/api/v1.0/precipitation')
def precipitation():

    session = Session(engine)
    query = session.query(measurement.date, func.sum(measurement.prcp))\
            .filter(measurement.date >= start_date)\
            .group_by(measurement.date)\
            .all()

    session.close()

    data = []
    for date, prcp in query:
        data_dict = {date: prcp}
        data.append(data_dict)

    return jsonify(data)


@app.route('/api/v1.0/stations')
def stations():

    session = Session(engine)
    query = session.query(measurement.station).distinct().all()

    session.close()

    data = []
    for station_name in query:
        data.append(station_name)

    return jsonify(data)


@app.route('/api/v1.0/tobs')
def tobss():

    session = Session(engine)

    query = session.query(station.name, measurement.station, func.count(measurement.station)) \
        .group_by(measurement.station) \
        .filter(station.station == measurement.station) \
        .filter(measurement.date >= start_date) \
        .order_by(func.count(measurement.station).desc()) \
        .limit(1) \
        .all()

    highest_station_name = query[0][1]

    station_df = session.query(measurement.tobs) \
        .filter(measurement.date >= start_date) \
        .filter(measurement.station == highest_station_name) \
        .all()

    session.close()

    data = []
    for tobs in station_df:
        data.append(tobs)

    return jsonify(data)


@app.route('/api/v1.0/<vacation_start_date>')
def start_date_func(vacation_start_date):
    session = Session(engine)

    vacation_start_date_lst = vacation_start_date.split('-')
    vacation_start_date_year = vacation_start_date_lst[0]

    vacation_start_date_month = vacation_start_date_lst[1]
    if vacation_start_date_month[0] == '0':
        vacation_start_date_month = vacation_start_date_month.replace('0', '')

    vacation_start_date_day = vacation_start_date_lst[2]
    if vacation_start_date_day[0] == '0':
        vacation_start_date_day = vacation_start_date_day.replace('0', '')

    clean_vacation_start_date = dt.date(int(vacation_start_date_year), int(vacation_start_date_month),
                                        int(vacation_start_date_day))

    result = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs))\
        .filter(measurement.date >= clean_vacation_start_date).all()

    session.close()

    return jsonify(result)


@app.route('/api/v1.0/<vacation_start_date>/<vacation_end_date>')
def start_end_date_fun(vacation_start_date, vacation_end_date):
    session = Session(engine)
    # ---------------------------- Start Date ----------------
    vacation_start_date_lst = vacation_start_date.split('-')
    vacation_start_date_year = vacation_start_date_lst[0]

    vacation_start_date_month = vacation_start_date_lst[1]
    if vacation_start_date_month[0] == '0':
        vacation_start_date_month = vacation_start_date_month.replace('0', '')

    vacation_start_date_day = vacation_start_date_lst[2]
    if vacation_start_date_day[0] == '0':
        vacation_start_date_day = vacation_start_date_day.replace('0', '')

    clean_vacation_start_date = dt.date(int(vacation_start_date_year), int(vacation_start_date_month),
                                        int(vacation_start_date_day))

    # --------------------------- End Date ----------------------
    vacation_end_date_lst = vacation_end_date.split('-')
    vacation_end_date_year = vacation_end_date_lst[0]

    vacation_end_date_month = vacation_end_date_lst[1]
    if vacation_end_date_month[0] == '0':
        vacation_end_date_month = vacation_end_date_month.replace('0', '')

    vacation_end_date_day = vacation_end_date_lst[2]
    if vacation_end_date_day[0] == '0':
        vacation_end_date_day = vacation_end_date_day.replace('0', '')

    clean_vacation_end_date = dt.date(int(vacation_end_date_year), int(vacation_end_date_month),
                                      int(vacation_end_date_day))

    # ---------------------- Query --------------------------------
    result = session.query(func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs)) \
        .filter(measurement.date >= clean_vacation_start_date)\
        .filter(measurement.date <= clean_vacation_end_date)\
        .all()

    session.close()

    return jsonify(result)


if __name__ == '__main__':
    app.run(debug=True)
