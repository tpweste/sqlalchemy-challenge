import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station


# Flask Setup

app = Flask(__name__)

# Flask Routes
#   * Home page.
#   * List all routes that are available.
@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )

#----------------------------------------------------------
# * `/api/v1.0/precipitation`
@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of prcp data including the date and prcp measurement for each date"""
    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).\
        filter(Measurement.date > "2016-08-22").\
        order_by(Measurement.date).all()

    session.close()

    # Convert the query results to a dictionary using `date` as the key and `prcp` as the value.
    measurement_dict = {}
    for date, prcp in results:
        measurement_dict[date] = prcp

    #   * Return the JSON representation of your dictionary.
    return jsonify(measurement_dict)


@app.route("/api/v1.0/stations")
def stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all station names"""
    # Query all stations
    results = session.query(Station.name).all()

    session.close()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    #   * Return a JSON list of stations from the dataset.
    return jsonify(all_stations)

#----------------------------------------------------------
# * `/api/v1.0/tobs`
@app.route("/api/v1.0/tobs")
def tobs():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all prcp data for the most active station"""
    # Query the dates and temperature observations of the most active station for the last year of data.
    most_active = session.query(Measurement.station, func.count(Measurement.station).label('total')).group_by(Measurement.station).order_by(text('total DESC')).first()[0]
    latest_date = session.query(Measurement.date).filter(Measurement.station == most_active).order_by(Measurement.date.desc()).first()._asdict()['date']
    latest_date_dt = dt.datetime.strptime(latest_date, '%Y-%m-%d')
    last_year = latest_date_dt - dt.timedelta(days=365)
    data = session.query(Measurement.tobs, Measurement.date).\
      filter(Measurement.date >= last_year, Measurement.station == most_active).\
      order_by(Measurement.date.desc()).all()

    session.close()
    
    measurement_dict = {}
    for tobs, date in data:
        measurement_dict[date] = tobs

    return jsonify(measurement_dict)
   


@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def stats(start=None, end=None):

    ''' Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
    When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive.'''
    session = Session(engine)

    if end:
        results = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).filter(Measurement.date <= end).group_by(Measurement.date).all()
    else:
        results = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
            filter(Measurement.date >= start).group_by(Measurement.date).all()
    
    results_dict = {
        'min_temp': results[0],
        'avg_temp': results[1],
        'max_temp': results[2]
    }

    session.close()
    return jsonify(results_dict)

if __name__ == '__main__':
    app.run(debug=True)