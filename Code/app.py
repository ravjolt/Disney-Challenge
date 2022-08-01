
import pandas as pd
from flask import (Flask, 
render_template, request)



# Set up Flask
app = Flask(__name__)

app.config['ENV'] = 'development'
app.config['DEBUG'] = True
app.config['TESTING'] = True

@app.route('/')
def home_page():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)