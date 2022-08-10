from flask import (Flask, render_template)
import sqlite3


app = Flask(__name__, template_folder= 'templates')
app.config['ENV'] = 'development'
app.config['DEBUG'] = True
app.config['TESTING'] = True


@app.route('/')
def home_page():
    return render_template("index.html")

@app.route('/Movies')
def movies_page():
    return render_template("movies.html")

@app.route('/Parks')
def park_page():
    def get_park_status():
        HOST_NAME = "localhost"
        HOST_PORT = 80
        dbfile = "disney_park.sqlite"
        conn = sqlite3.connect(dbfile)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM 'disney_park'")
        result = cursor.fetchall()
        conn.close()
        return result
    
    def get_ride_times():
        HOST_NAME = "localhost"
        HOST_PORT = 80
        dbfile = "disney_ride_wait.sqlite"
        conn = sqlite3.connect(dbfile)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM 'disney_ride_wait'")
        results = cursor.fetchall()
        conn.close()
        return results
    ride_times = get_ride_times()
    ride_status = get_park_status()
    return render_template("parks.html", ride=ride_times, status = ride_status)

if __name__ == '__main__':
  app.run(debug=True)